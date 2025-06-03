"""
Event Handlers for Migration System

This module provides event handlers that react to various events in the system.
Handlers can perform side effects, update read models, send notifications, etc.
"""

import logging
from typing import Dict, Any, List
from abc import ABC, abstractmethod
from datetime import datetime

from .event_models import (
    BaseEvent, EventType, MigrationStartedEvent, MigrationCompletedEvent,
    MigrationFailedEvent, DataExtractedEvent, DataTransformedEvent,
    DataValidatedEvent, DataImportedEvent, UserMigratedEvent,
    AdminMigratedEvent, RollbackInitiatedEvent, RollbackCompletedEvent
)


logger = logging.getLogger(__name__)


class EventHandler(ABC):
    """Base class for event handlers."""
    
    @abstractmethod
    async def handle(self, event: BaseEvent) -> None:
        """Handle an event."""
        pass
    
    @abstractmethod
    def can_handle(self, event_type: EventType) -> bool:
        """Check if this handler can handle the given event type."""
        pass


class MigrationEventHandler(EventHandler):
    """Handler for migration-related events."""
    
    def __init__(self):
        self._migration_stats: Dict[str, Dict[str, Any]] = {}
        self._user_migrations: Dict[str, Dict[str, Any]] = {}
        self._admin_migrations: Dict[str, Dict[str, Any]] = {}
    
    def can_handle(self, event_type: EventType) -> bool:
        """Check if this handler can handle the given event type."""
        return event_type in [
            EventType.MIGRATION_STARTED,
            EventType.MIGRATION_COMPLETED,
            EventType.MIGRATION_FAILED,
            EventType.DATA_EXTRACTED,
            EventType.DATA_TRANSFORMED,
            EventType.DATA_VALIDATED,
            EventType.DATA_IMPORTED,
            EventType.USER_MIGRATED,
            EventType.ADMIN_MIGRATED,
            EventType.ROLLBACK_INITIATED,
            EventType.ROLLBACK_COMPLETED
        ]
    
    async def handle(self, event: BaseEvent) -> None:
        """Handle migration events."""
        try:
            if event.event_type == EventType.MIGRATION_STARTED:
                await self._handle_migration_started(event)
            elif event.event_type == EventType.MIGRATION_COMPLETED:
                await self._handle_migration_completed(event)
            elif event.event_type == EventType.MIGRATION_FAILED:
                await self._handle_migration_failed(event)
            elif event.event_type == EventType.DATA_EXTRACTED:
                await self._handle_data_extracted(event)
            elif event.event_type == EventType.DATA_TRANSFORMED:
                await self._handle_data_transformed(event)
            elif event.event_type == EventType.DATA_VALIDATED:
                await self._handle_data_validated(event)
            elif event.event_type == EventType.DATA_IMPORTED:
                await self._handle_data_imported(event)
            elif event.event_type == EventType.USER_MIGRATED:
                await self._handle_user_migrated(event)
            elif event.event_type == EventType.ADMIN_MIGRATED:
                await self._handle_admin_migrated(event)
            elif event.event_type == EventType.ROLLBACK_INITIATED:
                await self._handle_rollback_initiated(event)
            elif event.event_type == EventType.ROLLBACK_COMPLETED:
                await self._handle_rollback_completed(event)
        
        except Exception as e:
            logger.error(f"Error handling event {event.event_type}: {e}")
            raise
    
    async def _handle_migration_started(self, event: BaseEvent) -> None:
        """Handle migration started event."""
        migration_id = event.aggregate_id
        self._migration_stats[migration_id] = {
            'status': 'started',
            'started_at': event.timestamp,
            'source_config': event.data.get('source_config', {}),
            'target_config': event.data.get('target_config', {}),
            'extracted_count': 0,
            'transformed_count': 0,
            'validated_count': 0,
            'imported_count': 0,
            'user_count': 0,
            'admin_count': 0,
            'errors': []
        }
        
        logger.info(f"Migration {migration_id} started")
    
    async def _handle_migration_completed(self, event: BaseEvent) -> None:
        """Handle migration completed event."""
        migration_id = event.aggregate_id
        
        if migration_id in self._migration_stats:
            self._migration_stats[migration_id].update({
                'status': 'completed',
                'completed_at': event.timestamp,
                'duration_seconds': event.data.get('duration_seconds', 0),
                'final_statistics': event.data.get('statistics', {})
            })
        
        logger.info(f"Migration {migration_id} completed successfully")
    
    async def _handle_migration_failed(self, event: BaseEvent) -> None:
        """Handle migration failed event."""
        migration_id = event.aggregate_id
        
        if migration_id in self._migration_stats:
            self._migration_stats[migration_id].update({
                'status': 'failed',
                'failed_at': event.timestamp,
                'error_message': event.data.get('error_message', ''),
                'error_details': event.data.get('error_details', {})
            })
        
        logger.error(f"Migration {migration_id} failed: {event.data.get('error_message', '')}")
    
    async def _handle_data_extracted(self, event: BaseEvent) -> None:
        """Handle data extracted event."""
        extraction_id = event.aggregate_id
        record_count = event.data.get('record_count', 0)
        
        # Find related migration and update stats
        correlation_id = event.correlation_id
        if correlation_id:
            migration_id = str(correlation_id)
            if migration_id in self._migration_stats:
                self._migration_stats[migration_id]['extracted_count'] += record_count
        
        logger.info(f"Data extraction {extraction_id} completed: {record_count} records")
    
    async def _handle_data_transformed(self, event: BaseEvent) -> None:
        """Handle data transformed event."""
        transformation_id = event.aggregate_id
        output_count = event.data.get('output_count', 0)
        
        # Find related migration and update stats
        correlation_id = event.correlation_id
        if correlation_id:
            migration_id = str(correlation_id)
            if migration_id in self._migration_stats:
                self._migration_stats[migration_id]['transformed_count'] += output_count
        
        logger.info(f"Data transformation {transformation_id} completed: {output_count} records")
    
    async def _handle_data_validated(self, event: BaseEvent) -> None:
        """Handle data validated event."""
        validation_id = event.aggregate_id
        valid_records = event.data.get('valid_records', 0)
        invalid_records = event.data.get('invalid_records', 0)
        
        # Find related migration and update stats
        correlation_id = event.correlation_id
        if correlation_id:
            migration_id = str(correlation_id)
            if migration_id in self._migration_stats:
                self._migration_stats[migration_id]['validated_count'] += valid_records
                if invalid_records > 0:
                    self._migration_stats[migration_id]['errors'].extend(
                        event.data.get('validation_errors', [])
                    )
        
        logger.info(f"Data validation {validation_id} completed: {valid_records} valid, {invalid_records} invalid")
    
    async def _handle_data_imported(self, event: BaseEvent) -> None:
        """Handle data imported event."""
        import_id = event.aggregate_id
        imported_count = event.data.get('imported_count', 0)
        
        # Find related migration and update stats
        correlation_id = event.correlation_id
        if correlation_id:
            migration_id = str(correlation_id)
            if migration_id in self._migration_stats:
                self._migration_stats[migration_id]['imported_count'] += imported_count
        
        logger.info(f"Data import {import_id} completed: {imported_count} records")
    
    async def _handle_user_migrated(self, event: BaseEvent) -> None:
        """Handle user migrated event."""
        user_id = event.aggregate_id
        username = event.data.get('username', '')
        
        self._user_migrations[user_id] = {
            'username': username,
            'migrated_at': event.timestamp,
            'source_data': event.data.get('source_data', {}),
            'target_data': event.data.get('target_data', {})
        }
        
        # Find related migration and update stats
        correlation_id = event.correlation_id
        if correlation_id:
            migration_id = str(correlation_id)
            if migration_id in self._migration_stats:
                self._migration_stats[migration_id]['user_count'] += 1
        
        logger.info(f"User {username} (ID: {user_id}) migrated successfully")
    
    async def _handle_admin_migrated(self, event: BaseEvent) -> None:
        """Handle admin migrated event."""
        admin_id = event.aggregate_id
        username = event.data.get('username', '')
        
        self._admin_migrations[admin_id] = {
            'username': username,
            'migrated_at': event.timestamp,
            'source_data': event.data.get('source_data', {}),
            'target_data': event.data.get('target_data', {})
        }
        
        # Find related migration and update stats
        correlation_id = event.correlation_id
        if correlation_id:
            migration_id = str(correlation_id)
            if migration_id in self._migration_stats:
                self._migration_stats[migration_id]['admin_count'] += 1
        
        logger.info(f"Admin {username} (ID: {admin_id}) migrated successfully")
    
    async def _handle_rollback_initiated(self, event: BaseEvent) -> None:
        """Handle rollback initiated event."""
        rollback_id = event.aggregate_id
        migration_id = event.data.get('migration_id', '')
        reason = event.data.get('reason', '')
        
        if migration_id in self._migration_stats:
            self._migration_stats[migration_id]['rollback_initiated'] = {
                'rollback_id': rollback_id,
                'reason': reason,
                'initiated_at': event.timestamp
            }
        
        logger.warning(f"Rollback {rollback_id} initiated for migration {migration_id}: {reason}")
    
    async def _handle_rollback_completed(self, event: BaseEvent) -> None:
        """Handle rollback completed event."""
        rollback_id = event.aggregate_id
        migration_id = event.data.get('migration_id', '')
        success = event.data.get('success', False)
        
        if migration_id in self._migration_stats:
            self._migration_stats[migration_id]['rollback_completed'] = {
                'rollback_id': rollback_id,
                'success': success,
                'completed_at': event.timestamp,
                'rollback_stats': event.data.get('rollback_stats', {})
            }
        
        status = "successfully" if success else "with errors"
        logger.info(f"Rollback {rollback_id} for migration {migration_id} completed {status}")
    
    def get_migration_stats(self, migration_id: str) -> Dict[str, Any]:
        """Get statistics for a specific migration."""
        return self._migration_stats.get(migration_id, {})
    
    def get_all_migration_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get statistics for all migrations."""
        return self._migration_stats.copy()
    
    def get_user_migrations(self) -> Dict[str, Dict[str, Any]]:
        """Get all user migrations."""
        return self._user_migrations.copy()
    
    def get_admin_migrations(self) -> Dict[str, Dict[str, Any]]:
        """Get all admin migrations."""
        return self._admin_migrations.copy()


class NotificationEventHandler(EventHandler):
    """Handler for sending notifications based on events."""
    
    def __init__(self, notification_service=None):
        self.notification_service = notification_service
    
    def can_handle(self, event_type: EventType) -> bool:
        """Check if this handler can handle the given event type."""
        return event_type in [
            EventType.MIGRATION_COMPLETED,
            EventType.MIGRATION_FAILED,
            EventType.ROLLBACK_INITIATED,
            EventType.ROLLBACK_COMPLETED
        ]
    
    async def handle(self, event: BaseEvent) -> None:
        """Handle notification events."""
        try:
            if event.event_type == EventType.MIGRATION_COMPLETED:
                await self._send_migration_completed_notification(event)
            elif event.event_type == EventType.MIGRATION_FAILED:
                await self._send_migration_failed_notification(event)
            elif event.event_type == EventType.ROLLBACK_INITIATED:
                await self._send_rollback_initiated_notification(event)
            elif event.event_type == EventType.ROLLBACK_COMPLETED:
                await self._send_rollback_completed_notification(event)
        
        except Exception as e:
            logger.error(f"Error sending notification for event {event.event_type}: {e}")
    
    async def _send_migration_completed_notification(self, event: BaseEvent) -> None:
        """Send notification for completed migration."""
        migration_id = event.aggregate_id
        duration = event.data.get('duration_seconds', 0)
        statistics = event.data.get('statistics', {})
        
        message = f"Migration {migration_id} completed successfully in {duration:.2f} seconds"
        logger.info(f"Notification: {message}")
        
        if self.notification_service:
            await self.notification_service.send_notification(
                title="Migration Completed",
                message=message,
                details=statistics
            )
    
    async def _send_migration_failed_notification(self, event: BaseEvent) -> None:
        """Send notification for failed migration."""
        migration_id = event.aggregate_id
        error_message = event.data.get('error_message', 'Unknown error')
        
        message = f"Migration {migration_id} failed: {error_message}"
        logger.error(f"Notification: {message}")
        
        if self.notification_service:
            await self.notification_service.send_notification(
                title="Migration Failed",
                message=message,
                details=event.data.get('error_details', {}),
                priority="high"
            )
    
    async def _send_rollback_initiated_notification(self, event: BaseEvent) -> None:
        """Send notification for initiated rollback."""
        rollback_id = event.aggregate_id
        migration_id = event.data.get('migration_id', '')
        reason = event.data.get('reason', '')
        
        message = f"Rollback {rollback_id} initiated for migration {migration_id}: {reason}"
        logger.warning(f"Notification: {message}")
        
        if self.notification_service:
            await self.notification_service.send_notification(
                title="Rollback Initiated",
                message=message,
                details=event.data.get('rollback_scope', {}),
                priority="high"
            )
    
    async def _send_rollback_completed_notification(self, event: BaseEvent) -> None:
        """Send notification for completed rollback."""
        rollback_id = event.aggregate_id
        migration_id = event.data.get('migration_id', '')
        success = event.data.get('success', False)
        
        status = "successfully" if success else "with errors"
        message = f"Rollback {rollback_id} for migration {migration_id} completed {status}"
        
        if success:
            logger.info(f"Notification: {message}")
        else:
            logger.error(f"Notification: {message}")
        
        if self.notification_service:
            await self.notification_service.send_notification(
                title="Rollback Completed",
                message=message,
                details=event.data.get('rollback_stats', {}),
                priority="medium" if success else "high"
            )


class AuditEventHandler(EventHandler):
    """Handler for audit logging of events."""
    
    def __init__(self, audit_logger=None):
        self.audit_logger = audit_logger or logger
    
    def can_handle(self, event_type: EventType) -> bool:
        """Check if this handler can handle the given event type."""
        # Audit all events
        return True
    
    async def handle(self, event: BaseEvent) -> None:
        """Handle audit logging for all events."""
        try:
            audit_entry = {
                'event_id': str(event.event_id),
                'event_type': event.event_type.value,
                'aggregate_id': event.aggregate_id,
                'aggregate_type': event.aggregate_type,
                'timestamp': event.timestamp.isoformat(),
                'correlation_id': str(event.correlation_id) if event.correlation_id else None,
                'causation_id': str(event.causation_id) if event.causation_id else None,
                'metadata': event.metadata,
                'data_summary': self._summarize_data(event.data)
            }
            
            self.audit_logger.info(f"AUDIT: {audit_entry}")
        
        except Exception as e:
            logger.error(f"Error in audit logging: {e}")
    
    def _summarize_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a summary of event data for audit purposes."""
        summary = {}
        
        for key, value in data.items():
            if isinstance(value, dict):
                summary[key] = f"<dict with {len(value)} keys>"
            elif isinstance(value, list):
                summary[key] = f"<list with {len(value)} items>"
            elif isinstance(value, str) and len(value) > 100:
                summary[key] = f"<string: {value[:100]}...>"
            else:
                summary[key] = value
        
        return summary