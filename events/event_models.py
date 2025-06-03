"""
Event Models for Event Sourcing

This module defines all event types used in the migration system.
Each event represents a significant state change or action in the system.
"""

from datetime import datetime
from typing import Any, Dict, Optional, Union
from uuid import UUID, uuid4
from pydantic import BaseModel, Field
from enum import Enum


class EventType(str, Enum):
    """Event types for the migration system."""
    MIGRATION_STARTED = "migration.started"
    MIGRATION_COMPLETED = "migration.completed"
    MIGRATION_FAILED = "migration.failed"
    DATA_EXTRACTED = "data.extracted"
    DATA_TRANSFORMED = "data.transformed"
    DATA_VALIDATED = "data.validated"
    DATA_IMPORTED = "data.imported"
    USER_MIGRATED = "user.migrated"
    ADMIN_MIGRATED = "admin.migrated"
    ROLLBACK_INITIATED = "rollback.initiated"
    ROLLBACK_COMPLETED = "rollback.completed"
    SCHEMA_COMPATIBILITY_CHECKED = "schema.compatibility.checked"
    UUID_TRANSFORMED = "uuid.transformed"
    DATETIME_CONVERTED = "datetime.converted"


class BaseEvent(BaseModel):
    """Base event model for all events in the system."""
    
    event_id: UUID = Field(default_factory=uuid4)
    event_type: EventType
    aggregate_id: str
    aggregate_type: str
    event_version: int = 1
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    correlation_id: Optional[UUID] = None
    causation_id: Optional[UUID] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    data: Dict[str, Any] = Field(default_factory=dict)
    
    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            UUID: lambda v: str(v)
        }


class MigrationStartedEvent(BaseEvent):
    """Event fired when a migration process starts."""
    
    event_type: EventType = EventType.MIGRATION_STARTED
    aggregate_type: str = "migration"
    
    def __init__(self, migration_id: str, source_config: Dict[str, Any], 
                 target_config: Dict[str, Any], **kwargs):
        super().__init__(
            aggregate_id=migration_id,
            data={
                "source_config": source_config,
                "target_config": target_config,
                "started_at": datetime.utcnow().isoformat()
            },
            **kwargs
        )


class MigrationCompletedEvent(BaseEvent):
    """Event fired when a migration process completes successfully."""
    
    event_type: EventType = EventType.MIGRATION_COMPLETED
    aggregate_type: str = "migration"
    
    def __init__(self, migration_id: str, statistics: Dict[str, Any], 
                 duration_seconds: float, **kwargs):
        super().__init__(
            aggregate_id=migration_id,
            data={
                "statistics": statistics,
                "duration_seconds": duration_seconds,
                "completed_at": datetime.utcnow().isoformat()
            },
            **kwargs
        )


class MigrationFailedEvent(BaseEvent):
    """Event fired when a migration process fails."""
    
    event_type: EventType = EventType.MIGRATION_FAILED
    aggregate_type: str = "migration"
    
    def __init__(self, migration_id: str, error_message: str, 
                 error_details: Dict[str, Any], **kwargs):
        super().__init__(
            aggregate_id=migration_id,
            data={
                "error_message": error_message,
                "error_details": error_details,
                "failed_at": datetime.utcnow().isoformat()
            },
            **kwargs
        )


class DataExtractedEvent(BaseEvent):
    """Event fired when data is extracted from source system."""
    
    event_type: EventType = EventType.DATA_EXTRACTED
    aggregate_type: str = "extraction"
    
    def __init__(self, extraction_id: str, source_type: str, 
                 record_count: int, extraction_stats: Dict[str, Any], **kwargs):
        super().__init__(
            aggregate_id=extraction_id,
            data={
                "source_type": source_type,
                "record_count": record_count,
                "extraction_stats": extraction_stats,
                "extracted_at": datetime.utcnow().isoformat()
            },
            **kwargs
        )


class DataTransformedEvent(BaseEvent):
    """Event fired when data transformation is completed."""
    
    event_type: EventType = EventType.DATA_TRANSFORMED
    aggregate_type: str = "transformation"
    
    def __init__(self, transformation_id: str, input_count: int, 
                 output_count: int, transformation_stats: Dict[str, Any], **kwargs):
        super().__init__(
            aggregate_id=transformation_id,
            data={
                "input_count": input_count,
                "output_count": output_count,
                "transformation_stats": transformation_stats,
                "transformed_at": datetime.utcnow().isoformat()
            },
            **kwargs
        )


class DataValidatedEvent(BaseEvent):
    """Event fired when data validation is completed."""
    
    event_type: EventType = EventType.DATA_VALIDATED
    aggregate_type: str = "validation"
    
    def __init__(self, validation_id: str, total_records: int, 
                 valid_records: int, invalid_records: int, 
                 validation_errors: list, **kwargs):
        super().__init__(
            aggregate_id=validation_id,
            data={
                "total_records": total_records,
                "valid_records": valid_records,
                "invalid_records": invalid_records,
                "validation_errors": validation_errors,
                "validated_at": datetime.utcnow().isoformat()
            },
            **kwargs
        )


class DataImportedEvent(BaseEvent):
    """Event fired when data is imported to target system."""
    
    event_type: EventType = EventType.DATA_IMPORTED
    aggregate_type: str = "import"
    
    def __init__(self, import_id: str, target_type: str, 
                 imported_count: int, import_stats: Dict[str, Any], **kwargs):
        super().__init__(
            aggregate_id=import_id,
            data={
                "target_type": target_type,
                "imported_count": imported_count,
                "import_stats": import_stats,
                "imported_at": datetime.utcnow().isoformat()
            },
            **kwargs
        )


class UserMigratedEvent(BaseEvent):
    """Event fired when a user is successfully migrated."""
    
    event_type: EventType = EventType.USER_MIGRATED
    aggregate_type: str = "user"
    
    def __init__(self, user_id: str, username: str, 
                 source_data: Dict[str, Any], target_data: Dict[str, Any], **kwargs):
        super().__init__(
            aggregate_id=user_id,
            data={
                "username": username,
                "source_data": source_data,
                "target_data": target_data,
                "migrated_at": datetime.utcnow().isoformat()
            },
            **kwargs
        )


class AdminMigratedEvent(BaseEvent):
    """Event fired when an admin is successfully migrated."""
    
    event_type: EventType = EventType.ADMIN_MIGRATED
    aggregate_type: str = "admin"
    
    def __init__(self, admin_id: str, username: str, 
                 source_data: Dict[str, Any], target_data: Dict[str, Any], **kwargs):
        super().__init__(
            aggregate_id=admin_id,
            data={
                "username": username,
                "source_data": source_data,
                "target_data": target_data,
                "migrated_at": datetime.utcnow().isoformat()
            },
            **kwargs
        )


class RollbackInitiatedEvent(BaseEvent):
    """Event fired when a rollback process is initiated."""
    
    event_type: EventType = EventType.ROLLBACK_INITIATED
    aggregate_type: str = "rollback"
    
    def __init__(self, rollback_id: str, migration_id: str, 
                 reason: str, rollback_scope: Dict[str, Any], **kwargs):
        super().__init__(
            aggregate_id=rollback_id,
            data={
                "migration_id": migration_id,
                "reason": reason,
                "rollback_scope": rollback_scope,
                "initiated_at": datetime.utcnow().isoformat()
            },
            **kwargs
        )


class RollbackCompletedEvent(BaseEvent):
    """Event fired when a rollback process is completed."""
    
    event_type: EventType = EventType.ROLLBACK_COMPLETED
    aggregate_type: str = "rollback"
    
    def __init__(self, rollback_id: str, migration_id: str, 
                 rollback_stats: Dict[str, Any], success: bool, **kwargs):
        super().__init__(
            aggregate_id=rollback_id,
            data={
                "migration_id": migration_id,
                "rollback_stats": rollback_stats,
                "success": success,
                "completed_at": datetime.utcnow().isoformat()
            },
            **kwargs
        )


class SchemaCompatibilityCheckedEvent(BaseEvent):
    """Event fired when schema compatibility is checked."""
    
    event_type: EventType = EventType.SCHEMA_COMPATIBILITY_CHECKED
    aggregate_type: str = "schema"
    
    def __init__(self, check_id: str, source_schema: Dict[str, Any], 
                 target_schema: Dict[str, Any], compatibility_result: Dict[str, Any], **kwargs):
        super().__init__(
            aggregate_id=check_id,
            data={
                "source_schema": source_schema,
                "target_schema": target_schema,
                "compatibility_result": compatibility_result,
                "checked_at": datetime.utcnow().isoformat()
            },
            **kwargs
        )


class UuidTransformedEvent(BaseEvent):
    """Event fired when UUIDs are transformed."""
    
    event_type: EventType = EventType.UUID_TRANSFORMED
    aggregate_type: str = "uuid_transformation"
    
    def __init__(self, transformation_id: str, source_format: str, 
                 target_format: str, transformation_count: int, **kwargs):
        super().__init__(
            aggregate_id=transformation_id,
            data={
                "source_format": source_format,
                "target_format": target_format,
                "transformation_count": transformation_count,
                "transformed_at": datetime.utcnow().isoformat()
            },
            **kwargs
        )


class DatetimeConvertedEvent(BaseEvent):
    """Event fired when datetime formats are converted."""
    
    event_type: EventType = EventType.DATETIME_CONVERTED
    aggregate_type: str = "datetime_conversion"
    
    def __init__(self, conversion_id: str, source_format: str, 
                 target_format: str, conversion_count: int, **kwargs):
        super().__init__(
            aggregate_id=conversion_id,
            data={
                "source_format": source_format,
                "target_format": target_format,
                "conversion_count": conversion_count,
                "converted_at": datetime.utcnow().isoformat()
            },
            **kwargs
        )