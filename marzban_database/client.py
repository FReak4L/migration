# migration/marzban_database/client.py

import asyncio
import json
import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncGenerator, Dict, List, Optional, Type

import pymysql
from sqlalchemy import select, func, insert, inspect as sa_inspect
from sqlalchemy.exc import SQLAlchemyError, IntegrityError, OperationalError
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker

from core.config import AppConfig
from core.exceptions import DatabaseError, MigrationError
from core.logging_setup import get_logger, RichText, get_console # type: ignore
from utils.datetime_utils import iso_format_to_datetime, unix_timestamp_to_datetime, datetime_to_unix_timestamp

# Import Marzban models from the same package
from .models import MarzbanBase, MarzbanUser, MarzbanAdmin, MarzbanProxy

logger = get_logger()
console = get_console()


class MigrationRollbackLog:
    """Logs actions performed during migration for potential rollback."""
    def __init__(self, db_client: 'MarzbanDatabaseClient'):
        self._actions: List[Tuple[str, int, Dict[str, Any]]] = []
        self.db_client = db_client

    def record_action(self, entity_type: str, entity_id: int, summary: Optional[Dict[str, Any]] = None) -> None:
        if not isinstance(entity_id, int) or entity_id <= 0:
            logger.warning(f"RollbackLog: Invalid entity_id ({entity_id}) for type '{entity_type}'. Action not logged.")
            return
        self._actions.append((entity_type.lower(), entity_id, summary or {}))
        logger.debug(f"Rollback action logged: Type='{entity_type}', ID={entity_id}, Summary={summary or {}}")

    async def trigger_rollback_procedures(self) -> None:
        if not self._actions:
            logger.info("No rollback actions recorded, or rollback already performed.")
            return
        logger.warning(RichText(f"🚨 Initiating rollback of [bold]{len(self._actions)}[/bold] database modifications due to migration failure... 🚨", style="bold red on yellow"))
        for entity_type, entity_id, entity_summary in reversed(self._actions):
            logger.info(f"Rolling back: Attempting to delete {entity_type} with ID {entity_id} (Summary: {entity_summary})...")
            if entity_type == "proxy" and "user_id_ref" in entity_summary:
                 logger.warning(f"Simplified proxy rollback for user_id {entity_summary['user_id_ref']} due to proxy entity_id being {entity_id} (which might be user_id). This needs more specific proxy ID handling for precise rollback if proxies have own PKs.")
                 pass # Actual deletion of proxies might happen via CASCADE on user delete, or need specific logic.
            else:
                await self.db_client._execute_rollback_deletion(entity_type, entity_id)
        logger.info(RichText("✅ Rollback procedure completed.", style="bold green"))
        self._actions.clear()


class MarzbanDatabaseClient:
    """Handles database operations for the target Marzban database."""
    def __init__(self, config: AppConfig):
        self.config = config
        if self.config.MARZBAN_DATABASE_TYPE == "mysql":
             pymysql.install_as_MySQLdb()
        self.engine = self._get_sqlalchemy_engine()
        self.AsyncSessionFactory = sessionmaker(
            bind=self.engine, class_=AsyncSession, expire_on_commit=False # type: ignore
        )
        self._user_id_map: Dict[str, int] = {}
        self._admin_id_map: Dict[str, int] = {}

    def _get_sqlalchemy_engine(self) -> Any:
        db_url: str
        engine_options: Dict[str, Any] = {"echo": self.config.LOG_LEVEL.upper() == "DEBUG", "future": True}

        if self.config.MARZBAN_DATABASE_TYPE == "sqlite":
            path_str = self.config.MARZBAN_DATABASE_PATH
            if not path_str:
                raise DatabaseError("SQLite MARZBAN_DATABASE_PATH is not set in configuration.")
            db_path_obj = Path(path_str)
            db_path_obj.parent.mkdir(parents=True, exist_ok=True)
            db_url = f"sqlite+aiosqlite:///{db_path_obj.resolve()}"
            logger.info(f"Target Marzban SQLite DB Engine URL: {db_url}")
        elif self.config.MARZBAN_DATABASE_TYPE == "mysql":
            db_url = (
                f"mysql+pymysql://{self.config.MARZBAN_MYSQL_USER}:{self.config.MARZBAN_MYSQL_PASSWORD.get_secret_value()}"
                f"@{self.config.MARZBAN_MYSQL_HOST}:{self.config.MARZBAN_MYSQL_PORT}"
                f"/{self.config.MARZBAN_MYSQL_DATABASE}?charset=utf8mb4"
            )
            engine_options.update({"pool_recycle": 3600, "pool_pre_ping": True, "pool_size": 5, "max_overflow": 10})
            logger.info(f"Target Marzban MySQL DB Engine: {self.config.MARZBAN_MYSQL_HOST}/{self.config.MARZBAN_MYSQL_DATABASE}")
        else:
            raise DatabaseError(f"Unsupported Marzban database type: {self.config.MARZBAN_DATABASE_TYPE}")
        return create_async_engine(db_url, **engine_options)

    @asynccontextmanager
    async def get_db_atomic_session(self) -> AsyncGenerator[AsyncSession, None]:
        session: AsyncSession = self.AsyncSessionFactory() # type: ignore
        session_id = id(session)
        try:
            async with session.begin():
                yield session
            logger.debug(f"DB Session {session_id} committed.")
        except SQLAlchemyError as e_sql:
            logger.error(f"DB Session {session_id} encountered SQLAlchemyError, transaction rolled back: {type(e_sql).__name__} - {str(e_sql)[:150]}", exc_info=(logger.getEffectiveLevel() <= logging.DEBUG))
            raise DatabaseError(f"Database operation failed: {e_sql}") from e_sql
        except Exception as e_generic:
            logger.error(f"DB Session {session_id} encountered unexpected error, attempting rollback: {type(e_generic).__name__} - {str(e_generic)[:150]}", exc_info=(logger.getEffectiveLevel() <= logging.DEBUG))
            try:
                if session.in_transaction(): await session.rollback()
            except Exception as e_rb:
                 logger.error(f"DB Session {session_id} critical error during explicit rollback attempt: {e_rb}", exc_info=True)
            raise MigrationError(f"Generic error during database session: {e_generic}") from e_generic
        finally:
            await session.close()
            logger.debug(f"DB Session {session_id} closed.")

    async def create_database_schema(self, drop_first: bool = False) -> None:
        logger.info(f"Initializing Marzban DB schema. Drop existing tables first: {drop_first}")
        async with self.engine.begin() as conn: # type: ignore
            if drop_first:
                logger.warning(RichText("⚠️  Dropping ALL existing Marzban tables! This is destructive. ⚠️", style="bold yellow on red"))
                await conn.run_sync(MarzbanBase.metadata.drop_all) # type: ignore
                logger.info("All existing Marzban tables dropped.")
            await conn.run_sync(MarzbanBase.metadata.create_all) # type: ignore
        logger.info(RichText("✅ Marzban DB schema initialization complete.", style="bold green"))
        self._user_id_map.clear()
        self._admin_id_map.clear()

    async def _insert_generic_batch(
        self,
        model_class: Type[MarzbanBase],
        model_name_str: str,
        batch_data_list: List[Dict[str, Any]],
        rollback_logger: MigrationRollbackLog,
        id_cache_map: Dict[str, int]
    ) -> int:
        newly_inserted_this_batch = 0
        if not batch_data_list: return 0

        valid_batch_for_insert: List[Dict[str, Any]] = []
        usernames_in_current_batch = set()
        model_columns = {col.name for col in sa_inspect(model_class).columns}

        for item_data_orig in batch_data_list:
            item_data_processed = dict(item_data_orig)
            username = item_data_processed.get("username")
            if not username:
                logger.warning(f"Skipping {model_name_str} record due to missing username: {item_data_orig}")
                continue
            
            for field_name in ["created_at", "password_reset_at", "edit_at", "sub_revoked_at",
                               "sub_updated_at", "online_at", "on_hold_timeout", "last_status_change", "expire"]:
                if field_name in item_data_processed:
                    val = item_data_processed[field_name]
                    if field_name == "expire":
                        if isinstance(val, (datetime, float)):
                            item_data_processed[field_name] = datetime_to_unix_timestamp(val) if isinstance(val, datetime) else int(val)
                        elif isinstance(val, str) and val.isdigit():
                            item_data_processed[field_name] = int(val)
                        elif not isinstance(val, (int, type(None))):
                            logger.warning(f"Invalid type or value for 'expire' field for {model_name_str} '{username}': {val} ({type(val)}). Setting to None.")
                            item_data_processed[field_name] = None
                    else:
                        if isinstance(val, (int, float)):
                            item_data_processed[field_name] = unix_timestamp_to_datetime(val)
                        elif isinstance(val, str):
                            item_data_processed[field_name] = iso_format_to_datetime(val)
                        elif not isinstance(val, (datetime, type(None))):
                            logger.warning(f"Invalid type for datetime field '{field_name}' in {model_name_str} '{username}': {type(val)}. Setting to None.")
                            item_data_processed[field_name] = None
            
            item_data_filtered = {k: v for k, v in item_data_processed.items() if k in model_columns}
            
            for col in sa_inspect(model_class).columns:
                if col.name in item_data_filtered and item_data_filtered[col.name] is not None:
                    if isinstance(col.type, (Integer, BigInteger)) and not isinstance(item_data_filtered[col.name], int):
                        try:
                            item_data_filtered[col.name] = int(item_data_filtered[col.name])
                        except (ValueError, TypeError):
                            logger.warning(f"Could not coerce value for column '{col.name}' to int for {model_name_str} '{username}'. Original: {item_data_filtered[col.name]}. Setting to None or default if applicable.")
                            item_data_filtered[col.name] = None
                    elif isinstance(col.type, String) and not isinstance(item_data_filtered[col.name], str):
                         item_data_filtered[col.name] = str(item_data_filtered[col.name])
            
            valid_batch_for_insert.append(item_data_filtered)
            usernames_in_current_batch.add(username)

        if not valid_batch_for_insert: return 0

        async with self.get_db_atomic_session() as session:
            existing_db_records: Dict[str, int] = {}
            if usernames_in_current_batch:
                stmt_existing = select(model_class.id, model_class.username).where(model_class.username.in_(list(usernames_in_current_batch))) #type: ignore
                result_existing = await session.execute(stmt_existing)
                for row_map in result_existing.mappings(): # type: ignore
                    existing_db_records[row_map["username"]] = row_map["id"] # type: ignore
                    if row_map["username"] not in id_cache_map:
                        id_cache_map[row_map["username"]] = row_map["id"] # type: ignore
                        logger.debug(f"Pre-check: Found existing {model_name_str} '{row_map['username']}' (ID: {row_map['id']}). Cache updated.")

            records_to_actually_insert = []
            for item_to_check in valid_batch_for_insert:
                uname_check = item_to_check["username"]
                if uname_check in existing_db_records or uname_check in id_cache_map:
                    if uname_check not in id_cache_map and uname_check in existing_db_records:
                         id_cache_map[uname_check] = existing_db_records[uname_check]
                    logger.info(f"{model_name_str.capitalize()} '{uname_check}' already exists with ID {id_cache_map.get(uname_check)}. Skipping insert.")
                else:
                    records_to_actually_insert.append(item_to_check)

            if not records_to_actually_insert:
                logger.debug(f"No new {model_name_str}s to insert in this batch after pre-check.")
                return 0

            try:
                stmt = insert(model_class).values(records_to_actually_insert)
                if self.config.MARZBAN_DATABASE_TYPE == "sqlite":
                    from sqlalchemy.dialects.sqlite import insert as sqlite_insert_stmt
                    stmt = sqlite_insert_stmt(model_class).values(records_to_actually_insert)
                    stmt = stmt.on_conflict_do_nothing(index_elements=['username'])
                
                await session.execute(stmt)

                for inserted_item_data in records_to_actually_insert:
                    uname_inserted = inserted_item_data["username"]
                    if uname_inserted not in id_cache_map: 
                        stmt_get_new_id = select(model_class.id).where(model_class.username == uname_inserted) # type: ignore
                        result_new_id = await session.execute(stmt_get_new_id)
                        new_id = result_new_id.scalar_one_or_none()
                        if new_id:
                            id_cache_map[uname_inserted] = new_id # type: ignore
                            rollback_logger.record_action(model_name_str.lower(), new_id, {"username": uname_inserted})
                            newly_inserted_this_batch += 1
                            logger.debug(f"Successfully inserted new {model_name_str} '{uname_inserted}' with ID {new_id}.")
                        else:
                            logger.warning(f"Could not retrieve ID for {model_name_str} '{uname_inserted}' post-insert attempt. It might have been skipped by DB due to ON CONFLICT or not inserted.")
            except IntegrityError as e_int:
                logger.error(f"Database IntegrityError during batch insert of {model_name_str}s: {str(e_int)[:200]}. Batch will be rolled back.", exc_info=(logger.getEffectiveLevel() <= logging.DEBUG))
                raise DatabaseError(f"Integrity error during {model_name_str} batch insert") from e_int
            except Exception as e_other:
                logger.error(f"Unexpected error during batch insert of {model_name_str}s: {type(e_other).__name__} - {str(e_other)[:200]}. Batch will be rolled back.", exc_info=(logger.getEffectiveLevel() <= logging.DEBUG))
                raise DatabaseError(f"Unexpected error during {model_name_str} batch insert") from e_other
        return newly_inserted_this_batch

    @async_retry_operation # type: ignore
    async def insert_admins_to_marzban(self, admin_data_list: List[Dict[str, Any]], rb_log: MigrationRollbackLog) -> int:
        return await self._insert_generic_batch(MarzbanAdmin, "Admin", admin_data_list, rb_log, self._admin_id_map)

    @async_retry_operation # type: ignore
    async def insert_users_to_marzban(self, user_data_list: List[Dict[str, Any]], rb_log: MigrationRollbackLog) -> int:
        return await self._insert_generic_batch(MarzbanUser, "User", user_data_list, rb_log, self._user_id_map)

    @async_retry_operation # type: ignore
    async def insert_proxies_to_marzban(self, proxy_data_list: List[Dict[str, Any]], rb_log: MigrationRollbackLog) -> int:
        inserted_count_total = 0
        skipped_proxies_no_user_id = 0
        records_to_insert_proxies: List[Dict[str, Any]] = []

        for proxy_item_orig in proxy_data_list:
            proxy_item = dict(proxy_item_orig)
            user_username_ref = proxy_item.pop("_username_ref", None)
            user_id = self._user_id_map.get(user_username_ref) if user_username_ref else None

            if user_id is None:
                logger.warning(f"Skipping proxy for user '{user_username_ref}' as user ID not found in cache. Proxy data: {proxy_item}")
                skipped_proxies_no_user_id += 1
                continue

            proxy_item["user_id"] = user_id
            if 'settings' in proxy_item:
                if isinstance(proxy_item['settings'], str):
                    try:
                        proxy_item['settings'] = json.loads(proxy_item['settings'])
                    except json.JSONDecodeError:
                        logger.error(f"Invalid JSON string in proxy settings for user '{user_username_ref}', type '{proxy_item.get('type')}'. Data: '{proxy_item['settings'][:70]}...'. Setting to empty dict.")
                        proxy_item['settings'] = {}
                elif not isinstance(proxy_item['settings'], dict):
                     logger.warning(f"Proxy settings for user '{user_username_ref}' is neither str nor dict ({type(proxy_item['settings'])}). Setting to empty dict.")
                     proxy_item['settings'] = {}
            else:
                proxy_item['settings'] = {}
            records_to_insert_proxies.append(proxy_item)

        if not records_to_insert_proxies:
            logger.info(f"No valid proxies to insert in this batch (total {len(proxy_data_list)} processed, {skipped_proxies_no_user_id} skipped).")
            return 0

        async with self.get_db_atomic_session() as session:
            try:
                await session.execute(insert(MarzbanProxy), records_to_insert_proxies) # type: ignore
                inserted_this_op = len(records_to_insert_proxies)
                inserted_count_total += inserted_this_op
                # For rollback, this is simplified. Ideally, capture actual inserted proxy IDs.
                for inserted_proxy in records_to_insert_proxies:
                    # Attempt to log a meaningful identifier for proxy rollback, e.g., using user_id and type
                    # This is a placeholder as precise proxy ID might not be available from bulk insert without RETURNING
                    rb_log.record_action("proxy_for_user", inserted_proxy["user_id"], {"type": inserted_proxy["type"], "user_id_ref": inserted_proxy["user_id"]})
                logger.debug(f"Successfully submitted insert for {inserted_this_op} proxies in current DB operation.")
            except IntegrityError as e_int_proxy:
                logger.error(f"Database IntegrityError during batch insert of proxies: {str(e_int_proxy)[:200]}. Batch will be rolled back.", exc_info=(logger.getEffectiveLevel() <= logging.DEBUG))
                raise DatabaseError("Integrity error during proxy batch insert") from e_int_proxy
            except Exception as e_other_proxy:
                logger.error(f"Unexpected error during batch insert of proxies: {type(e_other_proxy).__name__} - {str(e_other_proxy)[:200]}. Batch will be rolled back.", exc_info=(logger.getEffectiveLevel() <= logging.DEBUG))
                raise DatabaseError("Unexpected error during proxy batch insert") from e_other_proxy

        logger.info(f"Proxy batch insertion processing complete. Total proxies submitted for insert: {inserted_count_total} (out of {len(proxy_data_list)} provided, {skipped_proxies_no_user_id} skipped).")
        return inserted_count_total

    async def _execute_rollback_deletion(self, entity_model_name: str, entity_id: int) -> None:
        model_map = {"admin": MarzbanAdmin, "user": MarzbanUser, "proxy": MarzbanProxy}
        model_cls = model_map.get(entity_model_name.lower())
        if not model_cls:
            logger.error(f"Rollback: Unknown model type '{entity_model_name}' for entity ID {entity_id}. Cannot delete.")
            return
        async with self.get_db_atomic_session() as session:
            try:
                item_to_delete_stmt = select(model_cls).where(model_cls.id == entity_id) # type: ignore
                result = await session.execute(item_to_delete_stmt)
                item = result.scalar_one_or_none()
                if item:
                    await session.delete(item)
                    logger.info(f"Rollback: Successfully deleted {entity_model_name} with ID {entity_id}.")
                else:
                    logger.warning(f"Rollback: {entity_model_name} with ID {entity_id} not found in database. Cannot delete.")
            except Exception as e_rb_del:
                logger.error(f"Rollback Error: Failed to delete {entity_model_name} ID {entity_id}: {type(e_rb_del).__name__} - {e_rb_del}", exc_info=True)
