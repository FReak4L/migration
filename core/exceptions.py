# migration/core/exceptions.py

from typing import Optional

class MigrationError(Exception):
    """Base exception for migration-specific errors."""
    pass

class APIRequestError(MigrationError):
    """Custom exception for Marzneshin API request errors."""
    def __init__(self, message: str, status_code: Optional[int] = None, response_text: Optional[str] = None):
        super().__init__(message)
        self.status_code = status_code
        self.response_text = response_text

class DatabaseError(MigrationError):
    """Custom exception for Marzban database operation errors."""
    pass

class TransformationError(MigrationError):
    """Custom exception for data transformation errors."""
    pass
