# migration/extractor/data_extractor.py

import asyncio
import json
import logging
from typing import Any, Dict, List, Union, Optional

from core.config import AppConfig
from core.exceptions import MigrationError
from core.logging_setup import get_logger, get_console, RICH_AVAILABLE, RichText # type: ignore
from marzneshin_client.client import MarzneshinAPIClient

if RICH_AVAILABLE:
    from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeRemainingColumn, TimeElapsedColumn, TaskID
    from rich.table import Table
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

class MarzneshinDataExtractor:
    """Orchestrates the extraction of all necessary data from Marzneshin."""
    def __init__(self, client: MarzneshinAPIClient, config: AppConfig):
        self.client = client
        self.config = config

    async def extract_all_relevant_data(self) -> Dict[str, List[Dict[str, Any]]]:
        """Extracts admins, users, services, and inbounds from Marzneshin."""
        logger.info(RichText("Starting Marzneshin data extraction...", style="bold yellow"))
        extracted_data_dict: Dict[str, List[Dict[str, Any]]] = {}
        entity_names = ["admins", "users", "services", "inbounds"]
        results: List[Union[List[Dict[Any, Any]], Exception]]

        progress_bar_instance_args = [
            SpinnerColumn(), "[progress.description]{task.description}", BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("({task.completed} of {task.total})") if os.getenv("RICH_PROGRESS_TOTALS") else TextColumn("({task.completed})"),
            TimeRemainingColumn(), TimeElapsedColumn()
        ]
        progress_bar_instance_kwargs = {
            "console": console, "transient": False, "refresh_per_second": 4
        }
        
        progress_context: Any # To satisfy type checker for 'with' statement
        if RICH_AVAILABLE:
            progress_context = Progress(*progress_bar_instance_args, **progress_bar_instance_kwargs) # type: ignore
        else:
            from contextlib import nullcontext # Import locally if not globally available
            progress_context = nullcontext()


        with progress_context as progress: # type: ignore
            admin_task_id = progress.add_task("Admins", total=None, start=True) if RICH_AVAILABLE and progress else None
            user_task_id = progress.add_task("Users", total=None, start=True) if RICH_AVAILABLE and progress else None
            service_task_id = progress.add_task("Services", total=None, start=True) if RICH_AVAILABLE and progress else None
            inbound_task_id = progress.add_task("Inbounds", total=None, start=True) if RICH_AVAILABLE and progress else None

            async with self.client: # Uses client's context manager
                results = await asyncio.gather(
                    self.client.get_all_admins(progress, admin_task_id),
                    self.client.get_all_users_with_proxies(progress, user_task_id),
                    self.client.get_all_services(progress, service_task_id),
                    self.client.get_all_inbounds(progress, inbound_task_id),
                    return_exceptions=True
                )

        has_errors = False
        for i, entity_name in enumerate(entity_names):
            if isinstance(results[i], Exception):
                logger.error(f"Failed to extract {entity_name}: {type(results[i]).__name__} - {results[i]}")
                extracted_data_dict[entity_name] = []
                has_errors = True
            else:
                extracted_data_dict[entity_name] = results[i] if isinstance(results[i], list) else [] # type: ignore

        if has_errors:
            logger.warning("One or more data extraction tasks failed. Proceeding with successfully extracted data. Check previous errors for details.")

        if RICH_AVAILABLE:
            summary_table = Table(title=RichText("Extraction Summary",style="bold blue"), show_header=True, header_style="bold magenta", border_style="blue")
            summary_table.add_column("Entity", style="dim cyan", min_width=12)
            summary_table.add_column("Count Extracted", justify="right", style="green", min_width=15)
            for entity, items_list in extracted_data_dict.items():
                summary_table.add_row(entity.capitalize(), str(len(items_list)))
            console.print(summary_table)
        else:
            logger.info(f"Marzneshin extraction complete. Users: {len(extracted_data_dict.get('users',[]))}, Admins: {len(extracted_data_dict.get('admins',[]))}, Services: {len(extracted_data_dict.get('services',[]))}, Inbounds: {len(extracted_data_dict.get('inbounds',[]))}.")

        logger.info(RichText("Marzneshin extraction phase completed.", style="bold green"))
        return extracted_data_dict

    def save_data_as_json(self, data: Dict[str, List[Dict[str, Any]]], filepath_str: str) -> None:
        """Saves the provided data dictionary to a JSON file."""
        logger.info(f"Saving extracted Marzneshin data to '{filepath_str}'")
        file_path = Path(filepath_str)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(file_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False, default=str) # Use default=str for datetimes
            logger.info(f"Extracted data successfully saved to '{file_path.resolve()}'")
        except Exception as e:
            raise MigrationError(f"Error saving extracted data to JSON file '{filepath_str}': {e}") from e
