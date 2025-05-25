# migration/importer/data_importer.py

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

from sqlalchemy.exc import OperationalError # Already imported in marzban_database.client

from core.config import AppConfig
from core.exceptions import MigrationError, DatabaseError
from core.logging_setup import get_logger, get_console, RICH_AVAILABLE, RichText # type: ignore
from marzban_database.client import MarzbanDatabaseClient, MigrationRollbackLog

if RICH_AVAILABLE:
    from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeRemainingColumn, TimeElapsedColumn, TaskID
else: # Dummy progress related classes if Rich is not available
    TaskID = type("TaskID", (), {}) # type: ignore
    class Progress: # type: ignore
        def __init__(self, *args, **kwargs): pass
        def __enter__(self) -> 'Progress': return self
        def __exit__(self, *args: Any) -> None: pass
        def add_task(self, description: str, total: Optional[float] = None, start: bool = True) -> TaskID: return 0 # type: ignore
        def update(self, task_id: TaskID, advance: Optional[float] = None, completed: Optional[float] = None, total: Optional[float] = None, description: Optional[str] = None, visible: Optional[bool] = True) -> None: pass
        def start_task(self, task_id: TaskID) -> None: pass

logger = get_logger()
console = get_console()


class MarzbanDataImporter:
    """Orchestrates the import of transformed data into the Marzban database."""
    def __init__(self, db_client: MarzbanDatabaseClient, config: AppConfig):
        self.db_client = db_client
        self.config = config
        self.importer_rollback_log = MigrationRollbackLog(db_client)

    async def import_data_from_file(self, transformed_data_filepath: str) -> None:
        logger.info(RichText(f"Importing transformed data into Marzban from '{transformed_data_filepath}'", style="bold yellow"))
        try:
            file_path = Path(transformed_data_filepath)
            if not file_path.exists():
                raise MigrationError(f"Transformed data file not found: {file_path.resolve()}")
            with open(file_path, "r", encoding="utf-8") as f:
                data_to_import = json.load(f)
        except Exception as e:
            raise MigrationError(f"Error reading or parsing transformed data file '{transformed_data_filepath}': {e}") from e

        admins_to_import = data_to_import.get("admins", [])
        users_to_import = data_to_import.get("users", [])
        proxies_to_import = data_to_import.get("proxies", [])

        await self.db_client.create_database_schema(drop_first=True)

        total_progress_steps = (1 if admins_to_import else 0) + \
                               (1 if users_to_import else 0) + \
                               (1 if proxies_to_import else 0)
        
        progress_bar_instance_args = [
            SpinnerColumn(), "[progress.description]{task.description}", BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("({task.completed} of {task.total} batches)"),
            TimeRemainingColumn(), TimeElapsedColumn()
        ]
        progress_bar_instance_kwargs = {
            "console": console, "transient": False, "refresh_per_second": 2
        }
        
        progress_context: Any
        if RICH_AVAILABLE:
            progress_context = Progress(*progress_bar_instance_args, **progress_bar_instance_kwargs) # type: ignore
        else:
            from contextlib import nullcontext # Import locally if not globally available
            progress_context = nullcontext()

        with progress_context as progress_bar: # type: ignore
            main_import_overall_task_id = progress_bar.add_task("Overall DB Import Progress", total=total_progress_steps, start=False) if RICH_AVAILABLE and progress_bar else None
            if RICH_AVAILABLE and progress_bar and main_import_overall_task_id is not None : progress_bar.start_task(main_import_overall_task_id)

            try:
                if admins_to_import:
                    num_admin_batches = (len(admins_to_import) + self.config.DB_BATCH_SIZE - 1) // self.config.DB_BATCH_SIZE
                    task_id_admins = progress_bar.add_task(f"Importing {len(admins_to_import)} Admins...", total=num_admin_batches) if RICH_AVAILABLE and progress_bar else None
                    if RICH_AVAILABLE and progress_bar and task_id_admins is not None : progress_bar.start_task(task_id_admins)
                    for i in range(0, len(admins_to_import), self.config.DB_BATCH_SIZE):
                        batch = admins_to_import[i:i + self.config.DB_BATCH_SIZE]
                        await self.db_client.insert_admins_to_marzban(batch, self.importer_rollback_log)
                        if RICH_AVAILABLE and progress_bar and task_id_admins is not None: progress_bar.update(task_id_admins, advance=1, description=f"Importing Admins (Batch {i//self.config.DB_BATCH_SIZE + 1}/{num_admin_batches})")
                        if i + self.config.DB_BATCH_SIZE < len(admins_to_import): await asyncio.sleep(self.config.DB_BATCH_DELAY_S)
                    if RICH_AVAILABLE and progress_bar and task_id_admins is not None: progress_bar.update(task_id_admins, description=f"[green]Admins imported: {len(admins_to_import)}[/]", completed=num_admin_batches)
                    if RICH_AVAILABLE and progress_bar and main_import_overall_task_id is not None: progress_bar.update(main_import_overall_task_id, advance=1)

                if users_to_import:
                    num_user_batches = (len(users_to_import) + self.config.DB_BATCH_SIZE - 1) // self.config.DB_BATCH_SIZE
                    task_id_users = progress_bar.add_task(f"Importing {len(users_to_import)} Users...", total=num_user_batches) if RICH_AVAILABLE and progress_bar else None
                    if RICH_AVAILABLE and progress_bar and task_id_users is not None : progress_bar.start_task(task_id_users)
                    for i in range(0, len(users_to_import), self.config.DB_BATCH_SIZE):
                        batch = users_to_import[i:i + self.config.DB_BATCH_SIZE]
                        await self.db_client.insert_users_to_marzban(batch, self.importer_rollback_log)
                        if RICH_AVAILABLE and progress_bar and task_id_users is not None: progress_bar.update(task_id_users, advance=1, description=f"Importing Users (Batch {i//self.config.DB_BATCH_SIZE + 1}/{num_user_batches})")
                        if i + self.config.DB_BATCH_SIZE < len(users_to_import): await asyncio.sleep(self.config.DB_BATCH_DELAY_S)
                    if RICH_AVAILABLE and progress_bar and task_id_users is not None: progress_bar.update(task_id_users, description=f"[green]Users imported: {len(users_to_import)}[/]", completed=num_user_batches)
                    if RICH_AVAILABLE and progress_bar and main_import_overall_task_id is not None: progress_bar.update(main_import_overall_task_id, advance=1)

                if proxies_to_import:
                    num_proxy_batches = (len(proxies_to_import) + self.config.DB_BATCH_SIZE - 1) // self.config.DB_BATCH_SIZE
                    task_id_proxies = progress_bar.add_task(f"Importing {len(proxies_to_import)} Proxies...", total=num_proxy_batches) if RICH_AVAILABLE and progress_bar else None
                    if RICH_AVAILABLE and progress_bar and task_id_proxies is not None : progress_bar.start_task(task_id_proxies)
                    if not self.db_client._user_id_map and proxies_to_import :
                        logger.warning("User ID cache for Marzban DB is empty, but proxies are scheduled. This may mean no new users were inserted (all existed or other issues). Proxy import might fail or be incomplete.")
                    for i in range(0, len(proxies_to_import), self.config.DB_BATCH_SIZE):
                        batch = proxies_to_import[i:i + self.config.DB_BATCH_SIZE]
                        await self.db_client.insert_proxies_to_marzban(batch, self.importer_rollback_log)
                        if RICH_AVAILABLE and progress_bar and task_id_proxies is not None: progress_bar.update(task_id_proxies, advance=1, description=f"Importing Proxies (Batch {i//self.config.DB_BATCH_SIZE + 1}/{num_proxy_batches})")
                        if i + self.config.DB_BATCH_SIZE < len(proxies_to_import): await asyncio.sleep(self.config.DB_BATCH_DELAY_S)
                    if RICH_AVAILABLE and progress_bar and task_id_proxies is not None: progress_bar.update(task_id_proxies, description=f"[green]Proxies processed: {len(proxies_to_import)}[/]", completed=num_proxy_batches)
                    if RICH_AVAILABLE and progress_bar and main_import_overall_task_id is not None: progress_bar.update(main_import_overall_task_id, advance=1)

                logger.info(RichText("All Marzban data import operations submitted successfully.",style="bold green"))

            except (DatabaseError, MigrationError, OperationalError) as e_db_import_phase:
                logger.critical(RichText(f"Critical error during Marzban data import phase: {type(e_db_import_phase).__name__} - {e_db_import_phase}. Attempting rollback...",style="bold red on yellow"), exc_info=True)
                await self.importer_rollback_log.trigger_rollback_procedures()
                logger.info("Rollback procedure finished after import error.")
                raise
            except Exception as e_unexpected_import_phase:
                logger.critical(RichText(f"Unexpected critical error during Marzban data import: {type(e_unexpected_import_phase).__name__} - {e_unexpected_import_phase}",style="bold red on yellow"), exc_info=True)
                await self.importer_rollback_log.trigger_rollback_procedures()
                logger.info("Rollback procedure finished after unexpected import error.")
                raise MigrationError(f"Unexpected error during data import: {e_unexpected_import_phase}") from e_unexpected_import_phase
