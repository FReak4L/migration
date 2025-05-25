# migration/main_script.py

import asyncio
import logging
import os
import sys
import time
from datetime import timedelta
from pathlib import Path

# Core components
from core.config import get_app_configuration, AppConfig
from core.logging_setup import _setup_global_logger, get_logger, get_console, RICH_AVAILABLE, RichText # type: ignore
from core.exceptions import MigrationError, APIRequestError, DatabaseError, TransformationError

# Service components
from marzneshin_client.client import MarzneshinAPIClient
from extractor.data_extractor import MarzneshinDataExtractor
from transformer.data_transformer import DataTransformer
from marzban_database.client import MarzbanDatabaseClient
from importer.data_importer import MarzbanDataImporter # Assuming importer.py will be created from MarzbanDataImporter class
from validator.data_validator import DataValidator


if RICH_AVAILABLE:
    from rich.panel import Panel
else: # Dummy Panel if Rich is not available
    class Panel: # type: ignore
        def __init__(self, content: Any, title: str = "", border_style: str = "default", expand: bool = True, style: str = ""):
            print(f"--- {title} ---")
            print(str(content))
            print("-----------------")

# Initialize logger and console at the module level
logger = get_logger() # This will also initialize console if not already
console = get_console()

async def migration_main_logic() -> bool:
    """Main orchestration logic for the Marzneshin to Marzban migration."""
    start_timestamp = time.monotonic()
    cfg = get_app_configuration(logger_instance=logger, console_instance=console) # Pass instances

    console.print(Panel(RichText(f"🚀 Marzneshin -> Marzban Migration v{datetime.now().strftime('%Y%m%d_%H%M')} Initializing 🚀", style="bold blue"), border_style="blue", expand=False))
    logger.info(f"Source Marzneshin API Endpoint: {cfg.MARZNESHIN_API_URL}")
    logger.info(f"Target Marzban Database Type: {cfg.MARZBAN_DATABASE_TYPE}")
    if cfg.MARZBAN_DATABASE_TYPE == 'sqlite':
        logger.info(f"Target Marzban SQLite DB Path: {Path(cfg.MARZBAN_DATABASE_PATH).resolve() if cfg.MARZBAN_DATABASE_PATH else 'N/A'}")

    # Initialize components
    marzneshin_api_client = MarzneshinAPIClient(cfg)
    marzneshin_extractor = MarzneshinDataExtractor(marzneshin_api_client, cfg)
    data_transformer = DataTransformer(cfg)
    marzban_db_client = MarzbanDatabaseClient(cfg)
    # Assuming MarzbanDataImporter is in importer.data_importer module
    marzban_importer = MarzbanDataImporter(marzban_db_client, cfg)


    source_marzneshin_data: Optional[Dict[str, List[Dict[str, Any]]]] = None
    overall_migration_success = False
    migration_halted_prematurely = False

    try:
        # Step 1: Extract data from Marzneshin
        console.print(Panel(RichText("Step 1: Extracting Data from Marzneshin", style="bold cyan"), style="cyan"))
        source_marzneshin_data = await marzneshin_extractor.extract_all_relevant_data()
        marzneshin_extractor.save_data_as_json(source_marzneshin_data, cfg.MARZNESHIN_EXTRACTED_DATA_FILE)

        # Step 2: Transform data
        console.print(Panel(RichText("Step 2: Transforming Data for Marzban", style="bold cyan"), style="cyan"))
        transformed_data_for_marzban = data_transformer.orchestrate_transformation(source_marzneshin_data)
        transformed_data_file_path = Path(cfg.MARZBAN_TRANSFORMED_DATA_FILE)
        transformed_data_file_path.write_text(json.dumps(transformed_data_for_marzban, indent=2, default=str), encoding='utf-8')
        logger.info(f"Transformed data suitable for Marzban saved to '{transformed_data_file_path.resolve()}'")

        # Step 3: Import data into Marzban
        console.print(Panel(RichText("Step 3: Importing Data into Marzban Database", style="bold cyan"), style="cyan"))
        await marzban_importer.import_data_from_file(cfg.MARZBAN_TRANSFORMED_DATA_FILE)

        # Step 4: Post-migration validation
        console.print(Panel(RichText("Step 4: Performing Post-Migration Validation", style="bold cyan"), style="cyan"))
        if source_marzneshin_data and (source_marzneshin_data.get("users") is not None or source_marzneshin_data.get("admins") is not None):
            data_validator = DataValidator(source_marzneshin_data, marzban_db_client)
            validation_passed = await data_validator.run_all_validations()
            overall_migration_success = validation_passed
        else:
            logger.warning("Source data from Marzneshin was empty or unavailable after extraction. Cannot perform comprehensive validation.")
            overall_migration_success = False
    
    except KeyboardInterrupt:
        logger.info(RichText("🚨 Migration process manually interrupted by user (Ctrl+C). 🚨", style="bold yellow on red"))
        overall_migration_success = False
        migration_halted_prematurely = True
        if hasattr(marzban_importer, 'importer_rollback_log') and marzban_importer.importer_rollback_log._actions:
            logger.info("Attempting rollback due to interruption...")
            await marzban_importer.importer_rollback_log.trigger_rollback_procedures()
        raise
    except (MigrationError, APIRequestError, DatabaseError, TransformationError) as e_mig:
        logger.critical(f"Migration HALTED due to a critical error: {type(e_mig).__name__} - {e_mig}", exc_info=True)
        console.print(Panel(RichText(f"❌ Migration Halted: {type(e_mig).__name__} - {e_mig}", style="bold red"), border_style="red"))
        overall_migration_success = False
        migration_halted_prematurely = True
    except Exception as e_main_unexpected:
        logger.critical(f"An UNEXPECTED FATAL error occurred during migration: {type(e_main_unexpected).__name__} - {e_main_unexpected}", exc_info=True)
        console.print(Panel(RichText(f"❌ Migration Halted due to UNEXPECTED Error: {e_main_unexpected}", style="bold red"), border_style="red"))
        overall_migration_success = False
        migration_halted_prematurely = True
        if hasattr(marzban_importer, 'importer_rollback_log') and marzban_importer.importer_rollback_log._actions:
            logger.info("Attempting rollback due to unexpected error...")
            await marzban_importer.importer_rollback_log.trigger_rollback_procedures()
    finally:
        duration = timedelta(seconds=time.monotonic() - start_timestamp)
        final_panel_text = f"🏁 Migration Script Concluded. Total Time: {duration} 🏁"
        final_message_style = "bold yellow"

        if migration_halted_prematurely:
            final_panel_text = f"🏁 Migration Script Halted or Errored. Total Time: {duration}. 🏁"
            final_message_style = "bold red on yellow"
            logger.error("Migration was halted or finished with critical errors. Please review logs thoroughly.")
        elif overall_migration_success:
            final_message_style = "bold green"
            final_panel_text = f"🏁 Migration Script Finished Successfully (All Validations Passed). Total Time: {duration} 🏁"
            logger.info("Migration finished successfully and all validations passed.")
        else:
            final_panel_text = f"🏁 Migration Script Completed (but with Validation/Criteria Issues or Incomplete Validation). Total Time: {duration} 🏁"
            logger.warning("Migration completed, but with validation/criteria issues or incomplete validation. Please check logs for details.")
        
        console.print(Panel(RichText(final_panel_text, style=final_message_style), border_style=final_message_style.split(" ")[-1]))
        return overall_migration_success

# --- Script Entry Point ---
if __name__ == "__main__":
    # Logger and console are already initialized at module level via get_logger() and get_console()
    # Attempt to load .env configuration which might re-initialize logger with .env defined level
    try:
        # Pass the already initialized logger and console to get_app_configuration
        # This allows get_app_configuration to use them if it needs to log during its own setup,
        # and also allows it to fetch the LOG_LEVEL to potentially re-init the logger.
        config_settings = get_app_configuration(logger_instance=logger, console_instance=console)
        # If LOG_LEVEL from config is different than current logger, re-setup logger.
        # This is a bit tricky as _setup_global_logger is complex.
        # A simpler approach is that get_logger() itself can check config if logger is None.
        # For now, assume LOG_LEVEL from .env is respected if .env is loaded before first log.
        if logger.level != logging.getLevelName(config_settings.LOG_LEVEL.upper()):
             logger = _setup_global_logger(level_str=config_settings.LOG_LEVEL) # Re-init with new level

    except SystemExit:
        if logger: logger.info("Exiting due to configuration errors identified by get_app_configuration.")
        sys.exit(1)

    if not Path(".env").exists() and not all(os.getenv(var_name) for var_name in ["MARZNESHIN_API_URL", "MARZNESHIN_USERNAME", "MARZNESHIN_PASSWORD"]):
        logger.warning("'.env' file not found, and critical MARZNESHIN_ environment variables are not set. Relying on defaults or existing env vars.")

    exit_code = 1
    try:
        migration_succeeded = asyncio.run(migration_main_logic())
        exit_code = 0 if migration_succeeded else 1
    except KeyboardInterrupt:
        logger.info(RichText("🚨 Migration process manually interrupted by user (Ctrl+C in __main__). Exiting. 🚨", style="bold yellow on red"))
        exit_code = 130
    except SystemExit as e_sys_main:
        logger.info(f"SystemExit caught in __main__: code {e_sys_main.code}")
        exit_code = e_sys_main.code or 1
    except Exception as e_top_level_unexpected:
        logger.critical(f"💥 An unhandled top-level exception occurred in __main__: {type(e_top_level_unexpected).__name__} - {e_top_level_unexpected}", exc_info=True)
        console.print(Panel(RichText(f"💥 UNHANDLED FATAL ERROR: {e_top_level_unexpected}", style="bold white on red"), border_style="red", expand=False))
        exit_code = 2
    finally:
        final_fallback_logger = logger if logger and getattr(logger, '_initialized_fully', False) else logging.getLogger("MigrationMain_Fallback_Final")
        if not final_fallback_logger.hasHandlers() and not RICH_AVAILABLE:
            logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s | %(message)s (%(filename)s:%(lineno)d)')
            final_fallback_logger = logging.getLogger("MigrationMain_Fallback_Final")
        final_fallback_logger.info(f"Script __main__ execution block has concluded. Exiting with code {exit_code}.")
        logging.shutdown()
        sys.exit(exit_code)
