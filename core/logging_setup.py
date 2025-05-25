# migration/core/logging_setup.py

import logging
import sys
from pathlib import Path
from typing import Any, Optional

# Rich library for enhanced console output
try:
    from rich.logging import RichHandler
    from rich.console import Console
    from rich.text import Text as RichText # Renamed to avoid clash if other Text is used
    RICH_AVAILABLE = True
    console_instance = Console(stderr=True, highlight=False, log_time_format="[%X]")
except ImportError:
    RICH_AVAILABLE = False
    # Define dummy classes if rich is not available
    class DummyConsoleSafe:
        def print(self, *args, **kwargs): print(*args) #NOSONAR
        def rule(self, *args, **kwargs): print(f"--- {args[0] if args else ''} ---") #NOSONAR

    console_instance = DummyConsoleSafe() # type: ignore
    
    class RichText: # type: ignore
        def __init__(self, text: str, style: Optional[str] = None): self.text = text
        def __str__(self) -> str: return self.text

# Global logger instance, initialized by _setup_global_logger
logger: Optional[logging.Logger] = None

def _setup_global_logger(name: str = "MigrationMzsh2Mzb", level_str: str = "INFO") -> logging.Logger:
    global logger, console_instance # Use the module-level console_instance
    
    existing_logger = logging.getLogger(name)
    log_level_map = {"DEBUG": logging.DEBUG, "INFO": logging.INFO, "WARNING": logging.WARNING, "ERROR": logging.ERROR, "CRITICAL": logging.CRITICAL}
    effective_log_level = log_level_map.get(level_str.upper(), logging.INFO)

    # Check if logger is already fully initialized with the same settings
    if existing_logger.hasHandlers() and \
       existing_logger.level == effective_log_level and \
       getattr(existing_logger, '_initialized_fully', False):
        logger = existing_logger
        return logger

    log_instance = logging.getLogger(name)
    log_instance.setLevel(effective_log_level)
    log_instance.handlers.clear() # Clear any previous handlers
    log_instance.propagate = False # Prevent duplication if root logger is configured

    if RICH_AVAILABLE:
        # Ensure console_instance is a Rich Console
        if not isinstance(console_instance, Console):
            console_instance = Console(stderr=True, highlight=False, log_time_format="[%X]")
        
        rich_handler = RichHandler(
            console=console_instance, # type: ignore
            show_time=True, 
            show_level=True, 
            show_path=False, # Set to True to see file:lineno in console
            markup=True, 
            rich_tracebacks=True, 
            tracebacks_show_locals= (effective_log_level == logging.DEBUG),
            log_time_format="[%X]", 
            level=effective_log_level,
            keywords=[ # Keywords to highlight in logs
                "Marzneshin", "Marzban", "API", "DB", 
                "Extract", "Transform", "Import", "Validate", 
                "SUCCESS", "FAIL", "ERROR", "WARNING", "CRITICAL", "Rollback",
                "SKIP", "SKIPinsertING", "MISSING", "MISMATCH"
            ]
        )
        log_instance.addHandler(rich_handler)
    else: # Fallback to standard StreamHandler if Rich is not available
        if not isinstance(console_instance, DummyConsoleSafe): # type: ignore
            console_instance = DummyConsoleSafe() # type: ignore
            
        console_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)-8s | %(message)s (%(filename)s:%(lineno)d)', 
            datefmt='%H:%M:%S'
        )
        console_handler_stream = logging.StreamHandler(sys.stdout) # Or sys.stderr
        console_handler_stream.setLevel(effective_log_level)
        console_handler_stream.setFormatter(console_formatter)
        log_instance.addHandler(console_handler_stream)

    # File Handler Setup
    log_file_path_str = f"{name.lower().replace(' ', '_').replace('-', '_')}.log"
    try:
        # Ensure log directory exists (e.g., if logs/script_name.log is desired)
        # Path(log_file_path_str).parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_file_path_str, mode='a', encoding='utf-8')
        file_handler.setLevel(effective_log_level) # Log all levels to file as well, or choose a different one
        file_formatter = logging.Formatter(
            '%(asctime)s - %(levelname)-8s [%(name)s] | %(filename)s:%(lineno)s | %(funcName)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        file_handler.setFormatter(file_formatter)
        log_instance.addHandler(file_handler)
        
        if not getattr(log_instance, '_file_log_init_msg_sent', False):
            log_instance.info(f"File logging enabled at: {Path(log_file_path_str).resolve()}")
            setattr(log_instance, '_file_log_init_msg_sent', True)
            
    except Exception as e_file_log:
        log_instance.error(f"Failed to set up file logger ('{log_file_path_str}'): {e_file_log}. Console logging only.")

    logger = log_instance # Assign to global logger
    setattr(logger, '_initialized_fully', True) # Mark as fully initialized
    
    # Initial log message to confirm setup (only once)
    if not getattr(logger, '_overall_init_msg_sent', False):
        logger.info(f"Logger '{name}' initialized. Effective logging level: {logging.getLevelName(effective_log_level)}.")
        setattr(logger, '_overall_init_msg_sent', True)
        
    return logger

def get_logger() -> logging.Logger:
    """Returns the globally configured logger instance, initializing if necessary."""
    global logger
    if logger is None or not getattr(logger, '_initialized_fully', False) :
        # Attempt to get log level from config if available, otherwise default to INFO
        try:
            from .config import get_app_configuration
            cfg = get_app_configuration()
            log_level_from_config = cfg.LOG_LEVEL
        except Exception: # Broad catch if config isn't ready or fails
            log_level_from_config = "INFO"
        logger = _setup_global_logger(level_str=log_level_from_config)
    return logger # type: ignore

def get_console() -> Any:
    """Returns the globally configured console instance (Rich or Dummy)."""
    global console_instance
    # Ensure console_instance is initialized if it hasn't been by logger setup yet
    if console_instance is None:
        if RICH_AVAILABLE:
            console_instance = Console(stderr=True, highlight=False, log_time_format="[%X]")
        else:
            console_instance = DummyConsoleSafe()
    return console_instance
