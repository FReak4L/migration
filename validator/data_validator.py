# migration/validator/data_validator.py

import json
import logging
import os
from collections import defaultdict
from typing import Any, Dict, List

from sqlalchemy import select, func

from core.logging_setup import get_logger, get_console, RICH_AVAILABLE, RichText # type: ignore
from marzban_database.models import MarzbanUser, MarzbanAdmin, MarzbanProxy, MarzbanBase
from marzban_database.client import MarzbanDatabaseClient # To access _get_db_atomic_session
from transformer.data_transformer import DataTransformer # For _convert_key_to_uuid_format

if RICH_AVAILABLE:
    from rich.table import Table
    from rich.panel import Panel
else: # Dummy classes if Rich is not available
    class Table: # type: ignore
        def __init__(self, *args, **kwargs): self.title = kwargs.get("title"); self._rows = []
        def add_column(self, *args, **kwargs): pass
        def add_row(self, *args, **kwargs): self._rows.append(args)
        def __str__(self):
            header = f"{self.title}\n" if self.title else ""
            return header + "\n".join([" | ".join(map(str,r)) for r in self._rows])
    class Panel: # type: ignore
        def __init__(self, content: Any, title: str = "", border_style: str = "default", expand: bool = True, style: str = ""):
            print(f"--- {title} ---")
            print(str(content))
            print("-----------------")


logger = get_logger()
console = get_console()

class DataValidator:
    """Validates data consistency between source (Marzneshin) and target (Marzban)."""
    def __init__(self, mzsh_source_data: Dict[str, List[Dict[str, Any]]], mzb_target_db_client: MarzbanDatabaseClient):
        self.source_data = mzsh_source_data
        self.target_db = mzb_target_db_client
        self.source_counts: Dict[str, int] = {
            "users": len(self.source_data.get("users", [])),
            "admins": len(self.source_data.get("admins", []))
        }
        expected_proxy_count = 0
        self.source_user_proxy_map: Dict[str, Dict[str, str]] = defaultdict(dict)
        # Need a DataTransformer instance to use its UUID formatting method for consistent comparison
        # This assumes DataTransformer can be instantiated with the same config as target_db
        _temp_transformer = DataTransformer(self.target_db.config)


        for user_data_item in self.source_data.get("users", []):
            username = user_data_item.get("username")
            if not username: continue
            user_proxies_to_validate: Dict[str, str] = {}
            proxies_field_data = user_data_item.get("proxies", {})
            key_field_data = user_data_item.get("key")
            found_explicit_in_proxies_obj = False

            if isinstance(proxies_field_data, dict):
                for protocol_key, detail_dict in proxies_field_data.items():
                    if isinstance(detail_dict, dict) and detail_dict.get("id"):
                        user_proxies_to_validate[protocol_key.upper()] = detail_dict["id"]
                        found_explicit_in_proxies_obj = True
            if not found_explicit_in_proxies_obj and key_field_data:
                formatted_key_uuid = _temp_transformer._convert_key_to_uuid_format(key_field_data)
                if formatted_key_uuid:
                    user_proxies_to_validate["VLESS"] = formatted_key_uuid
            if user_proxies_to_validate:
                self.source_user_proxy_map[username] = user_proxies_to_validate
                expected_proxy_count += len(user_proxies_to_validate)
        self.source_counts["proxies"] = expected_proxy_count

    async def _count_entities_in_marzban_db(self, model_class: Type[MarzbanBase]) -> int:
        async with self.target_db.get_db_atomic_session() as session:
            count_statement = select(func.count(model_class.id)) # type: ignore
            result = await session.execute(count_statement)
            return result.scalar_one() or 0

    async def validate_counts(self) -> bool:
        logger.info("Validating entity counts (Source Marzneshin vs Target Marzban DB)...")
        all_counts_match = True
        validation_summary_table = None
        if RICH_AVAILABLE:
            validation_summary_table = Table(title=RichText("Entity Count Validation Summary",style="bold blue"), show_header=True, header_style="bold magenta", border_style="blue") #type: ignore
            validation_summary_table.add_column("Entity", style="dim cyan", min_width=12) #type: ignore
            validation_summary_table.add_column("Source Count (Mzsh)", justify="right", style="yellow", min_width=20) #type: ignore
            validation_summary_table.add_column("Target DB Count (Mzb)", justify="right", style="yellow", min_width=20) #type: ignore
            validation_summary_table.add_column("Status", justify="center", min_width=10) #type: ignore

        entity_model_map = {"users": MarzbanUser, "admins": MarzbanAdmin, "proxies": MarzbanProxy}
        for entity_key, source_count_val in self.source_counts.items():
            model_to_count = entity_model_map.get(entity_key)
            if not model_to_count:
                logger.warning(f"No Marzban model defined for validation count of entity '{entity_key}'. Skipping.")
                continue
            db_count_val = await self._count_entities_in_marzban_db(model_to_count)
            status_text = "MATCH" if source_count_val == db_count_val else "MISMATCH"
            status_style = "bold green" if source_count_val == db_count_val else "bold red"
            if RICH_AVAILABLE and validation_summary_table:
                validation_summary_table.add_row(entity_key.capitalize(), str(source_count_val), str(db_count_val), RichText(status_text, style=status_style)) #type: ignore
            else:
                logger.info(f"{entity_key.title()} Count: Source={source_count_val}, DB={db_count_val} -> {status_text}")
            if source_count_val != db_count_val:
                logger.error(f"{entity_key.title()} Count MISMATCH: Source Marzneshin reports {source_count_val}, but Marzban DB has {db_count_val}.")
                all_counts_match = False

        if RICH_AVAILABLE and validation_summary_table: console.print(validation_summary_table)
        if not all_counts_match:
            logger.error(RichText("❌ One or more critical entity count mismatches detected during validation.", style="bold red"))
            return False
        logger.info(RichText("✅ Entity count validation successful. All compared counts match.", style="bold green"))
        return True

    async def validate_uuids(self) -> bool:
        logger.info("Validating UUID preservation for user proxies...")
        overall_uuid_match = True
        if not self.source_user_proxy_map:
            logger.info("No source user proxy UUIDs were mapped from Marzneshin data. Skipping UUID validation.")
            return True
        mismatched_uuid_details_list: List[str] = []
        async with self.target_db.get_db_atomic_session() as session:
            db_users_query = await session.execute(select(MarzbanUser.id, MarzbanUser.username))
            marzban_users_id_map = {user_row.username: user_row.id for user_row in db_users_query.mappings()} #type: ignore
            db_proxies_query = await session.execute(select(MarzbanProxy.user_id, MarzbanProxy.type, MarzbanProxy.settings))
            marzban_proxies_by_user_id: Dict[int, List[Dict[str, Any]]] = defaultdict(list)
            for proxy_row in db_proxies_query.mappings(): #type: ignore
                marzban_proxies_by_user_id[proxy_row.user_id].append(dict(proxy_row)) #type: ignore

        for mzsh_username, source_protocol_to_uuid_map in self.source_user_proxy_map.items():
            marzban_user_id = marzban_users_id_map.get(mzsh_username)
            if not marzban_user_id:
                err_msg = f"User '{mzsh_username}': Not found in Marzban DB for UUID validation."
                logger.error(f"UUID Validation Error: {err_msg}")
                mismatched_uuid_details_list.append(err_msg); overall_uuid_match = False; continue
            marzban_db_proxies_for_user = marzban_proxies_by_user_id.get(marzban_user_id, [])
            actual_db_protocol_to_uuid_map: Dict[str, str] = {}
            for db_proxy_entry in marzban_db_proxies_for_user:
                proxy_type_db = str(db_proxy_entry.get('type', '')).upper()
                proxy_settings_db = db_proxy_entry.get('settings')
                if not proxy_type_db: continue
                db_uuid = None
                if isinstance(proxy_settings_db, dict) and proxy_settings_db.get('id'):
                    db_uuid = proxy_settings_db['id']
                elif isinstance(proxy_settings_db, str):
                    try:
                        settings_dict = json.loads(proxy_settings_db)
                        if isinstance(settings_dict, dict) and settings_dict.get('id'):
                            db_uuid = settings_dict['id']
                    except json.JSONDecodeError:
                        logger.warning(f"Could not parse settings JSON string for proxy {proxy_type_db} of user {mzsh_username} during UUID validation: {proxy_settings_db[:50]}")
                if db_uuid: actual_db_protocol_to_uuid_map[proxy_type_db] = db_uuid
            for expected_protocol, expected_source_uuid in source_protocol_to_uuid_map.items():
                expected_protocol_upper = expected_protocol.upper()
                if expected_protocol_upper not in actual_db_protocol_to_uuid_map:
                    err_msg = f"User '{mzsh_username}', Protocol '{expected_protocol_upper}': Proxy MISSING in Marzban DB (Expected Source UUID: {expected_source_uuid})"
                    logger.error(f"UUID Validation Error: {err_msg}")
                    mismatched_uuid_details_list.append(err_msg); overall_uuid_match = False
                elif actual_db_protocol_to_uuid_map[expected_protocol_upper] != expected_source_uuid:
                    err_msg = f"User '{mzsh_username}', Protocol '{expected_protocol_upper}': UUID MISMATCH. Source: {expected_source_uuid}, DB: {actual_db_protocol_to_uuid_map[expected_protocol_upper]}"
                    logger.error(f"UUID Validation Error: {err_msg}")
                    mismatched_uuid_details_list.append(err_msg); overall_uuid_match = False
        if overall_uuid_match:
            logger.info(RichText("✅ UUID preservation validation successful. All checked UUIDs match or source had no UUIDs to check.", style="bold green"))
        else:
            logger.error(RichText("❌ UUID preservation validation FAILED. Mismatches or missing proxies found.", style="bold red"))
            if mismatched_uuid_details_list and RICH_AVAILABLE:
                console.print(Panel(RichText("\n".join(mismatched_uuid_details_list[:15]), style="yellow"), title="[bold red]UUID Mismatch/Missing Details (Sample)[/]", border_style="red", expand=False)) # type: ignore
        return overall_uuid_match

    async def run_all_validations(self) -> bool:
        console.print(Panel(RichText("🔍 Running Post-Migration Validations 🔍", style="bold yellow"), border_style="yellow", expand=False)) #type: ignore
        counts_are_valid = await self.validate_counts()
        uuids_are_valid = await self.validate_uuids()
        final_validation_status = counts_are_valid and uuids_are_valid
        if final_validation_status:
            console.print(Panel(RichText("🎉 All primary validations passed successfully! Migration data appears consistent based on current checks.", style="bold green"), border_style="green")) #type: ignore
        else:
            console.print(Panel(RichText("❌ One or more validations FAILED. Please review the logs above carefully for details.", style="bold red"), border_style="red")) #type: ignore
        return final_validation_status
