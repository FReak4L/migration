# migration/core/config.py

import logging
import sys
from pathlib import Path
from typing import Optional

from decouple import config as decouple_config, UndefinedValueError
from pydantic import (
    BaseModel, Field, HttpUrl, SecretStr, field_validator, model_validator, ValidationError
)

# Avoid circular import with logging_setup by getting logger instance here if needed
# For now, this module should not log directly during class definition or get_app_configuration initial call.
# Logging will be done by the caller or by _setup_global_logger.


class AppConfig(BaseModel):
    MARZNESHIN_API_URL: HttpUrl
    MARZNESHIN_USERNAME: str
    MARZNESHIN_PASSWORD: SecretStr
    MARZBAN_DATABASE_TYPE: str = Field(default="sqlite")
    MARZBAN_DATABASE_PATH: Optional[str] = Field(default="/var/lib/marzban/db.sqlite3")
    MARZBAN_MYSQL_HOST: Optional[str] = Field(default="localhost")
    MARZBAN_MYSQL_PORT: Optional[int] = Field(default=3306)
    MARZBAN_MYSQL_USER: Optional[str] = Field(default="root")
    MARZBAN_MYSQL_PASSWORD: Optional[SecretStr] = Field(default=SecretStr(""))
    MARZBAN_MYSQL_DATABASE: Optional[str] = Field(default="marzban")
    LOG_LEVEL: str = Field(default="INFO")
    RETRY_ATTEMPTS: int = Field(default=3, ge=1)
    API_BATCH_SIZE: int = Field(default=100, ge=1)
    DB_BATCH_SIZE: int = Field(default=50, ge=1)
    API_REQUEST_DELAY_S: float = Field(default=0.1, ge=0)
    DB_BATCH_DELAY_S: float = Field(default=0.1, ge=0)
    MARZNESHIN_EXTRACTED_DATA_FILE: str = Field(default="marzneshin_extracted_data.json")
    MARZBAN_TRANSFORMED_DATA_FILE: str = Field(default="marzban_transformed_data.json")

    model_config = {"validate_assignment": True}

    @field_validator("MARZBAN_DATABASE_TYPE")
    @classmethod
    def validate_db_type(cls, value: str) -> str:
        supported_types = ["sqlite", "mysql"]
        if value.lower() not in supported_types:
            raise ValueError(f"Unsupported MARZBAN_DATABASE_TYPE: '{value}'. Must be one of {supported_types}.")
        return value.lower()

    @model_validator(mode="after")
    def check_conditional_db_config(self) -> 'AppConfig':
        if self.MARZBAN_DATABASE_TYPE == "sqlite" and not self.MARZBAN_DATABASE_PATH:
            raise ValueError("MARZBAN_DATABASE_PATH is required when MARZBAN_DATABASE_TYPE is 'sqlite'.")
        if self.MARZBAN_DATABASE_TYPE == "mysql":
            if not all([self.MARZBAN_MYSQL_HOST, self.MARZBAN_MYSQL_USER, self.MARZBAN_MYSQL_DATABASE]):
                raise ValueError("MARZBAN_MYSQL_HOST, MARZBAN_MYSQL_USER, and MARZBAN_MYSQL_DATABASE are required for 'mysql' type.")
        return self

_app_config_instance: Optional[AppConfig] = None

def get_app_configuration(logger_instance: Optional[logging.Logger] = None, console_instance: Optional[Any] = None) -> AppConfig:
    global _app_config_instance
    
    # Fallback basic print if logger/console not available during critical config error
    def _critical_print(message: str):
        if console_instance and hasattr(console_instance, 'print'):
            # Assuming Rich console or similar
            console_instance.print(f"[bold red]CRITICAL CONFIGURATION ERROR:[/bold red] {message}")
        else:
            print(f"CRITICAL CONFIGURATION ERROR: {message}", file=sys.stderr)

    if _app_config_instance:
        return _app_config_instance
    try:
        marzneshin_api_url_str = decouple_config("MARZNESHIN_API_URL")
        marzneshin_username_str = decouple_config("MARZNESHIN_USERNAME")
        marzneshin_password_str = decouple_config("MARZNESHIN_PASSWORD")
        
        _app_config_instance = AppConfig(
            MARZNESHIN_API_URL=HttpUrl(marzneshin_api_url_str), #type: ignore
            MARZNESHIN_USERNAME=marzneshin_username_str,
            MARZNESHIN_PASSWORD=SecretStr(marzneshin_password_str),
            MARZBAN_DATABASE_TYPE=decouple_config("MARZBAN_DATABASE_TYPE", default="sqlite"),
            MARZBAN_DATABASE_PATH=decouple_config("MARZBAN_DATABASE_PATH", default="/var/lib/marzban/db.sqlite3"),
            MARZBAN_MYSQL_HOST=decouple_config("MARZBAN_MYSQL_HOST", default="localhost"),
            MARZBAN_MYSQL_PORT=decouple_config("MARZBAN_MYSQL_PORT", cast=int, default=3306),
            MARZBAN_MYSQL_USER=decouple_config("MARZBAN_MYSQL_USER", default="root"),
            MARZBAN_MYSQL_PASSWORD=decouple_config("MARZBAN_MYSQL_PASSWORD", cast=SecretStr, default=SecretStr("")),
            MARZBAN_MYSQL_DATABASE=decouple_config("MARZBAN_MYSQL_DATABASE", default="marzban"),
            LOG_LEVEL=decouple_config("LOG_LEVEL", default="INFO"),
            RETRY_ATTEMPTS=decouple_config("RETRY_ATTEMPTS", cast=int, default=3),
            API_BATCH_SIZE=decouple_config("API_BATCH_SIZE", cast=int, default=100),
            DB_BATCH_SIZE=decouple_config("DB_BATCH_SIZE", cast=int, default=50),
            API_REQUEST_DELAY_S=decouple_config("API_REQUEST_DELAY_S", cast=float, default=0.1),
            DB_BATCH_DELAY_S=decouple_config("DB_BATCH_DELAY_S", cast=float, default=0.1),
            MARZNESHIN_EXTRACTED_DATA_FILE=decouple_config("MARZNESHIN_EXTRACTED_DATA_FILE", default="marzneshin_extracted_data.json"),
            MARZBAN_TRANSFORMED_DATA_FILE=decouple_config("MARZBAN_TRANSFORMED_DATA_FILE", default="marzban_transformed_data.json"),
        )
        if logger_instance: # Only log if logger is passed and initialized
            # Assuming RichText is available if logger setup is Rich-based
            try:
                from .logging_setup import RichText # Try local import
                logger_instance.info(RichText("Application configuration loaded and validated successfully.", style="bold green")) # type: ignore
            except ImportError: # Fallback if RichText not available here
                 logger_instance.info("Application configuration loaded and validated successfully.")
        
    except UndefinedValueError as e_uv:
        _critical_print(f"Missing required environment variable: '{e_uv.option_name}'. Please ensure it's set in your .env file or environment.")
        sys.exit(1)
    except ValidationError as e_val:
        _critical_print(f"Configuration validation error: {e_val}")
        sys.exit(1)
    except Exception as e_config:
        _critical_print(f"An unexpected error occurred during configuration loading: {type(e_config).__name__} - {e_config}")
        sys.exit(1)
    return _app_config_instance
