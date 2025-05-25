# migration/utils/retry_decorator.py

import asyncio
import logging
from functools import wraps
from typing import Any, Callable, Optional, Tuple, Type

import httpx # Assuming httpx is a direct dependency for retryable exceptions
from sqlalchemy.exc import OperationalError # Assuming direct import is okay here

# Import AppConfig and exceptions from core
# These will require the core package to be in PYTHONPATH or installed
from core.config import get_app_configuration 
from core.exceptions import APIRequestError, DatabaseError
from core.logging_setup import get_logger

logger = get_logger()

def async_retry_operation(
    func: Optional[Callable[..., Any]] = None, *, max_attempts: Optional[int] = None,
    initial_delay_s: float = 1.0, backoff_multiplier: float = 1.5, max_delay_s: float = 30.0,
    retryable_exceptions: Tuple[Type[Exception], ...] = (
        APIRequestError, DatabaseError, httpx.RequestError, httpx.TimeoutException, asyncio.TimeoutError, OperationalError
    )
) -> Callable[..., Any]:
    """
    A decorator for retrying an async function if it raises specified exceptions.
    Uses configuration for default max_attempts if not provided.
    """
    def decorator(decorated_func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(decorated_func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            cfg = get_app_configuration(logger_instance=logger) # Pass logger if config logs
            num_attempts = max_attempts if max_attempts is not None else cfg.RETRY_ATTEMPTS
            current_delay = initial_delay_s
            last_exception_caught: Optional[Exception] = None

            for attempt_num in range(num_attempts):
                try:
                    return await decorated_func(*args, **kwargs)
                except retryable_exceptions as e:
                    last_exception_caught = e
                    if attempt_num < num_attempts - 1:
                        logger.warning(f"Operation '{decorated_func.__name__}' attempt {attempt_num + 1}/{num_attempts} failed. Error: {type(e).__name__} - {str(e)[:150]}. Retrying in {current_delay:.2f}s...")
                        await asyncio.sleep(current_delay)
                        current_delay = min(current_delay * backoff_multiplier, max_delay_s)
                    else:
                        logger.error(f"Operation '{decorated_func.__name__}' failed after {num_attempts} attempts. Final error: {type(e).__name__} - {str(e)[:150]}")
                        raise e
                except Exception as e_unexpected:
                    current_logger_level = logger.getEffectiveLevel() if logger else logging.INFO
                    logger.error(f"Operation '{decorated_func.__name__}' encountered an unexpected (non-retryable) error on attempt {attempt_num + 1}/{num_attempts}: {type(e_unexpected).__name__} - {str(e_unexpected)[:150]}", exc_info=(current_logger_level <= logging.DEBUG))
                    raise e_unexpected

            if last_exception_caught:
                raise last_exception_caught
            return None # Should not be reached if an error occurred and was re-raised
        return wrapper
    return decorator(func) if func else decorator
