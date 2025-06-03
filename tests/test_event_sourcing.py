"""
Tests for Event Sourcing System

This module contains comprehensive tests for the event sourcing functionality,
including event store, event bus, event handlers, and event replay.
"""

import pytest
import pytest_asyncio
import asyncio
from datetime import datetime, timedelta
from uuid import uuid4, UUID
from typing import List, Dict, Any

from events import (
    EventStore, EventBus, EventStoreConfig, EventBusConfig,
    BaseEvent, EventType, MigrationStartedEvent, MigrationCompletedEvent,
    MigrationFailedEvent, DataExtractedEvent, DataTransformedEvent,
    UserMigratedEvent, AdminMigratedEvent, RollbackInitiatedEvent,
    MigrationEventHandler, NotificationEventHandler, AuditEventHandler,
    EventReplay, EventReplayConfig
)


class TestEventModels:
    """Test event model classes."""
    
    def test_base_event_creation(self):
        """Test creating a base event."""
        event = BaseEvent(
            event_type=EventType.MIGRATION_STARTED,
            aggregate_id="test-migration-1",
            aggregate_type="migration",
            data={"test": "data"},
            metadata={"test": "metadata"}
        )
        
        assert event.event_type == EventType.MIGRATION_STARTED
        assert event.aggregate_id == "test-migration-1"
        assert event.aggregate_type == "migration"
        assert event.data == {"test": "data"}
        assert event.metadata == {"test": "metadata"}
        assert isinstance(event.event_id, UUID)
        assert isinstance(event.timestamp, datetime)
    
    def test_migration_started_event(self):
        """Test migration started event."""
        source_config = {"host": "source.example.com"}
        target_config = {"host": "target.example.com"}
        
        event = MigrationStartedEvent(
            migration_id="test-migration-1",
            source_config=source_config,
            target_config=target_config
        )
        
        assert event.event_type == EventType.MIGRATION_STARTED
        assert event.aggregate_id == "test-migration-1"
        assert event.aggregate_type == "migration"
        assert event.data["source_config"] == source_config
        assert event.data["target_config"] == target_config
    
    def test_migration_completed_event(self):
        """Test migration completed event."""
        statistics = {"users": 100, "admins": 5}
        duration = 120.5
        
        event = MigrationCompletedEvent(
            migration_id="test-migration-1",
            statistics=statistics,
            duration_seconds=duration
        )
        
        assert event.event_type == EventType.MIGRATION_COMPLETED
        assert event.data["statistics"] == statistics
        assert event.data["duration_seconds"] == duration
    
    def test_user_migrated_event(self):
        """Test user migrated event."""
        source_data = {"id": 1, "username": "testuser"}
        target_data = {"id": "uuid-123", "username": "testuser"}
        
        event = UserMigratedEvent(
            user_id="uuid-123",
            username="testuser",
            source_data=source_data,
            target_data=target_data
        )
        
        assert event.event_type == EventType.USER_MIGRATED
        assert event.aggregate_id == "uuid-123"
        assert event.data["username"] == "testuser"
        assert event.data["source_data"] == source_data
        assert event.data["target_data"] == target_data


class TestInMemoryEventStore:
    """Test in-memory event store implementation."""
    
    @pytest_asyncio.fixture
    async def event_store(self):
        """Create an in-memory event store."""
        store = EventStore(use_memory=True)
        await store.initialize()
        yield store
        await store.close()
    
    @pytest.mark.asyncio
    async def test_append_and_get_events(self, event_store):
        """Test appending and retrieving events."""
        # Create test events
        event1 = BaseEvent(
            event_type=EventType.MIGRATION_STARTED,
            aggregate_id="test-migration-1",
            aggregate_type="migration",
            event_version=1
        )
        
        event2 = BaseEvent(
            event_type=EventType.DATA_EXTRACTED,
            aggregate_id="test-migration-1",
            aggregate_type="migration",
            event_version=2,
            correlation_id=event1.event_id
        )
        
        # Append events
        await event_store.append_event(event1)
        await event_store.append_event(event2)
        
        # Retrieve events
        events = await event_store.get_events("test-migration-1")
        
        assert len(events) == 2
        assert events[0].event_id == event1.event_id
        assert events[1].event_id == event2.event_id
        assert events[1].correlation_id == event1.event_id
    
    @pytest.mark.asyncio
    async def test_append_events_batch(self, event_store):
        """Test appending multiple events in a batch."""
        events = []
        for i in range(5):
            event = BaseEvent(
                event_type=EventType.USER_MIGRATED,
                aggregate_id=f"user-{i}",
                aggregate_type="user",
                data={"username": f"user{i}"}
            )
            events.append(event)
        
        await event_store.append_events(events)
        
        # Verify all events were stored
        for i, event in enumerate(events):
            stored_events = await event_store.get_events(f"user-{i}")
            assert len(stored_events) == 1
            assert stored_events[0].event_id == event.event_id
    
    @pytest.mark.asyncio
    async def test_get_events_by_type(self, event_store):
        """Test retrieving events by type."""
        # Create events of different types
        migration_event = BaseEvent(
            event_type=EventType.MIGRATION_STARTED,
            aggregate_id="test-migration-1",
            aggregate_type="migration"
        )
        
        user_event = BaseEvent(
            event_type=EventType.USER_MIGRATED,
            aggregate_id="user-1",
            aggregate_type="user"
        )
        
        await event_store.append_events([migration_event, user_event])
        
        # Get migration events
        migration_events = await event_store.get_events_by_type(EventType.MIGRATION_STARTED)
        assert len(migration_events) == 1
        assert migration_events[0].event_id == migration_event.event_id
        
        # Get user events
        user_events = await event_store.get_events_by_type(EventType.USER_MIGRATED)
        assert len(user_events) == 1
        assert user_events[0].event_id == user_event.event_id
    
    @pytest.mark.asyncio
    async def test_get_events_by_correlation_id(self, event_store):
        """Test retrieving events by correlation ID."""
        correlation_id = uuid4()
        
        event1 = BaseEvent(
            event_type=EventType.MIGRATION_STARTED,
            aggregate_id="test-migration-1",
            aggregate_type="migration",
            correlation_id=correlation_id
        )
        
        event2 = BaseEvent(
            event_type=EventType.DATA_EXTRACTED,
            aggregate_id="extraction-1",
            aggregate_type="extraction",
            correlation_id=correlation_id
        )
        
        await event_store.append_events([event1, event2])
        
        # Get events by correlation ID
        correlated_events = await event_store.get_events_by_correlation_id(correlation_id)
        assert len(correlated_events) == 2
        
        event_ids = [event.event_id for event in correlated_events]
        assert event1.event_id in event_ids
        assert event2.event_id in event_ids


class TestInMemoryEventBus:
    """Test in-memory event bus implementation."""
    
    @pytest_asyncio.fixture
    async def event_bus(self):
        """Create an in-memory event bus."""
        bus = EventBus(use_memory=True)
        await bus.start()
        yield bus
        await bus.stop()
    
    @pytest.mark.asyncio
    async def test_publish_and_subscribe(self, event_bus):
        """Test publishing and subscribing to events."""
        received_events = []
        
        async def handler(event: BaseEvent):
            received_events.append(event)
        
        # Subscribe to migration events
        await event_bus.subscribe([EventType.MIGRATION_STARTED], handler)
        
        # Publish an event
        event = BaseEvent(
            event_type=EventType.MIGRATION_STARTED,
            aggregate_id="test-migration-1",
            aggregate_type="migration"
        )
        
        await event_bus.publish(event)
        
        # Give some time for async processing
        await asyncio.sleep(0.1)
        
        assert len(received_events) == 1
        assert received_events[0].event_id == event.event_id
    
    @pytest.mark.asyncio
    async def test_publish_batch(self, event_bus):
        """Test publishing multiple events in a batch."""
        received_events = []
        
        async def handler(event: BaseEvent):
            received_events.append(event)
        
        # Subscribe to user events
        await event_bus.subscribe([EventType.USER_MIGRATED], handler)
        
        # Create multiple events
        events = []
        for i in range(3):
            event = BaseEvent(
                event_type=EventType.USER_MIGRATED,
                aggregate_id=f"user-{i}",
                aggregate_type="user"
            )
            events.append(event)
        
        await event_bus.publish_batch(events)
        
        # Give some time for async processing
        await asyncio.sleep(0.1)
        
        assert len(received_events) == 3
        
        received_ids = [event.event_id for event in received_events]
        for event in events:
            assert event.event_id in received_ids


class TestEventHandlers:
    """Test event handler implementations."""
    
    @pytest.mark.asyncio
    async def test_migration_event_handler(self):
        """Test migration event handler."""
        handler = MigrationEventHandler()
        
        # Test migration started
        migration_id = "test-migration-1"
        started_event = MigrationStartedEvent(
            migration_id=migration_id,
            source_config={"host": "source.example.com"},
            target_config={"host": "target.example.com"}
        )
        
        await handler.handle(started_event)
        
        stats = handler.get_migration_stats(migration_id)
        assert stats["status"] == "started"
        assert stats["extracted_count"] == 0
        
        # Test data extracted
        extracted_event = DataExtractedEvent(
            extraction_id="extraction-1",
            source_type="marzneshin",
            record_count=100,
            extraction_stats={},
            correlation_id=started_event.event_id
        )
        
        await handler.handle(extracted_event)
        
        stats = handler.get_migration_stats(migration_id)
        assert stats["extracted_count"] == 100
        
        # Test user migrated
        user_event = UserMigratedEvent(
            user_id="user-1",
            username="testuser",
            source_data={"id": 1},
            target_data={"id": "uuid-123"},
            correlation_id=started_event.event_id
        )
        
        await handler.handle(user_event)
        
        stats = handler.get_migration_stats(migration_id)
        assert stats["user_count"] == 1
        
        user_migrations = handler.get_user_migrations()
        assert "user-1" in user_migrations
        assert user_migrations["user-1"]["username"] == "testuser"
    
    @pytest.mark.asyncio
    async def test_notification_event_handler(self):
        """Test notification event handler."""
        notifications = []
        
        class MockNotificationService:
            async def send_notification(self, title, message, details=None, priority="normal"):
                notifications.append({
                    "title": title,
                    "message": message,
                    "details": details,
                    "priority": priority
                })
        
        handler = NotificationEventHandler(MockNotificationService())
        
        # Test migration completed notification
        completed_event = MigrationCompletedEvent(
            migration_id="test-migration-1",
            statistics={"users": 100},
            duration_seconds=120.5
        )
        
        await handler.handle(completed_event)
        
        assert len(notifications) == 1
        assert notifications[0]["title"] == "Migration Completed"
        assert "test-migration-1" in notifications[0]["message"]
        
        # Test migration failed notification
        failed_event = MigrationFailedEvent(
            migration_id="test-migration-2",
            error_message="Database connection failed",
            error_details={"code": "DB_ERROR"}
        )
        
        await handler.handle(failed_event)
        
        assert len(notifications) == 2
        assert notifications[1]["title"] == "Migration Failed"
        assert notifications[1]["priority"] == "high"
    
    @pytest.mark.asyncio
    async def test_audit_event_handler(self):
        """Test audit event handler."""
        audit_logs = []
        
        class MockAuditLogger:
            def info(self, message):
                audit_logs.append(message)
        
        handler = AuditEventHandler(MockAuditLogger())
        
        # Test audit logging
        event = BaseEvent(
            event_type=EventType.MIGRATION_STARTED,
            aggregate_id="test-migration-1",
            aggregate_type="migration",
            data={"large_data": "x" * 200}  # Large data to test summarization
        )
        
        await handler.handle(event)
        
        assert len(audit_logs) == 1
        assert "AUDIT:" in audit_logs[0]
        assert event.event_id.hex in audit_logs[0]


class TestEventReplay:
    """Test event replay functionality."""
    
    @pytest_asyncio.fixture
    async def event_store_with_data(self):
        """Create an event store with test data."""
        store = EventStore(use_memory=True)
        await store.initialize()
        
        # Add test events
        migration_id = "test-migration-1"
        correlation_id = uuid4()
        
        events = [
            MigrationStartedEvent(
                migration_id=migration_id,
                source_config={},
                target_config={},
                correlation_id=correlation_id
            ),
            DataExtractedEvent(
                extraction_id="extraction-1",
                source_type="marzneshin",
                record_count=50,
                extraction_stats={},
                correlation_id=correlation_id
            ),
            DataTransformedEvent(
                transformation_id="transform-1",
                input_count=50,
                output_count=50,
                transformation_stats={},
                correlation_id=correlation_id
            ),
            UserMigratedEvent(
                user_id="user-1",
                username="testuser1",
                source_data={},
                target_data={},
                correlation_id=correlation_id
            ),
            UserMigratedEvent(
                user_id="user-2",
                username="testuser2",
                source_data={},
                target_data={},
                correlation_id=correlation_id
            ),
            MigrationCompletedEvent(
                migration_id=migration_id,
                statistics={"users": 2},
                duration_seconds=60.0,
                correlation_id=correlation_id
            )
        ]
        
        await store.append_events(events)
        
        yield store, correlation_id
        await store.close()
    
    @pytest.mark.asyncio
    async def test_replay_all_events(self, event_store_with_data):
        """Test replaying all events."""
        store, correlation_id = event_store_with_data
        
        replay = EventReplay(store)
        handler = MigrationEventHandler()
        replay.add_handler(handler)
        
        result = await replay.replay_all_events()
        
        assert result.total_events == 6
        assert result.processed_events == 6
        assert result.failed_events == 0
        assert result.success_rate == 100.0
        
        # Check that handler processed events correctly
        stats = handler.get_migration_stats("test-migration-1")
        assert stats["status"] == "completed"
        assert stats["user_count"] == 2
    
    @pytest.mark.asyncio
    async def test_replay_aggregate_events(self, event_store_with_data):
        """Test replaying events for a specific aggregate."""
        store, correlation_id = event_store_with_data
        
        replay = EventReplay(store)
        handler = MigrationEventHandler()
        replay.add_handler(handler)
        
        result = await replay.replay_aggregate_events("test-migration-1")
        
        # Should replay migration events (started and completed)
        assert result.processed_events >= 2
        assert result.failed_events == 0
        
        stats = handler.get_migration_stats("test-migration-1")
        assert stats["status"] == "completed"
    
    @pytest.mark.asyncio
    async def test_replay_events_by_type(self, event_store_with_data):
        """Test replaying events by type."""
        store, correlation_id = event_store_with_data
        
        replay = EventReplay(store)
        handler = MigrationEventHandler()
        replay.add_handler(handler)
        
        result = await replay.replay_events_by_type([EventType.USER_MIGRATED])
        
        # Should replay only user migration events
        assert result.processed_events == 2
        assert result.failed_events == 0
        
        user_migrations = handler.get_user_migrations()
        assert len(user_migrations) == 2
        assert "user-1" in user_migrations
        assert "user-2" in user_migrations
    
    @pytest.mark.asyncio
    async def test_replay_events_by_correlation_id(self, event_store_with_data):
        """Test replaying events by correlation ID."""
        store, correlation_id = event_store_with_data
        
        replay = EventReplay(store)
        handler = MigrationEventHandler()
        replay.add_handler(handler)
        
        result = await replay.replay_events_by_correlation_id(correlation_id)
        
        # Should replay all events with the correlation ID
        assert result.processed_events == 6
        assert result.failed_events == 0
        
        stats = handler.get_migration_stats("test-migration-1")
        assert stats["status"] == "completed"
        assert stats["user_count"] == 2
    
    @pytest.mark.asyncio
    async def test_create_snapshot(self, event_store_with_data):
        """Test creating an aggregate snapshot."""
        store, correlation_id = event_store_with_data
        
        replay = EventReplay(store)
        
        snapshot = await replay.create_snapshot("test-migration-1")
        
        assert snapshot["aggregate_id"] == "test-migration-1"
        assert "snapshot_time" in snapshot
        assert "state" in snapshot
        assert snapshot["state"]["status"] == "completed"
        assert snapshot["state"]["user_count"] == 2
    
    @pytest.mark.asyncio
    async def test_validate_event_sequence(self, event_store_with_data):
        """Test validating event sequence."""
        store, correlation_id = event_store_with_data
        
        replay = EventReplay(store)
        
        # Test valid sequence
        expected_sequence = [
            EventType.MIGRATION_STARTED,
            EventType.MIGRATION_COMPLETED
        ]
        
        result = await replay.validate_event_sequence("test-migration-1", expected_sequence)
        
        assert result["aggregate_id"] == "test-migration-1"
        assert result["is_valid"] == True
        assert len(result["differences"]) == 0
        
        # Test invalid sequence
        invalid_sequence = [
            EventType.MIGRATION_COMPLETED,
            EventType.MIGRATION_STARTED
        ]
        
        result = await replay.validate_event_sequence("test-migration-1", invalid_sequence)
        
        assert result["is_valid"] == False
        assert len(result["differences"]) > 0


class TestEventSourcingIntegration:
    """Integration tests for the complete event sourcing system."""
    
    @pytest.mark.asyncio
    async def test_end_to_end_migration_flow(self):
        """Test a complete migration flow with event sourcing."""
        # Initialize components
        event_store = EventStore(use_memory=True)
        await event_store.initialize()
        
        event_bus = EventBus(use_memory=True)
        await event_bus.start()
        
        # Set up handlers
        migration_handler = MigrationEventHandler()
        notification_handler = NotificationEventHandler()
        
        # Subscribe handlers
        await event_bus.subscribe(
            [et for et in EventType],
            migration_handler.handle
        )
        
        await event_bus.subscribe(
            [EventType.MIGRATION_COMPLETED, EventType.MIGRATION_FAILED],
            notification_handler.handle
        )
        
        try:
            # Simulate migration flow
            migration_id = "integration-test-migration"
            correlation_id = uuid4()
            
            # 1. Start migration
            started_event = MigrationStartedEvent(
                migration_id=migration_id,
                source_config={"host": "source.example.com"},
                target_config={"host": "target.example.com"},
                correlation_id=correlation_id
            )
            
            await event_store.append_event(started_event)
            await event_bus.publish(started_event)
            
            # 2. Extract data
            extracted_event = DataExtractedEvent(
                extraction_id="extraction-1",
                source_type="marzneshin",
                record_count=100,
                extraction_stats={"duration": 10.5},
                correlation_id=correlation_id
            )
            
            await event_store.append_event(extracted_event)
            await event_bus.publish(extracted_event)
            
            # 3. Transform data
            transformed_event = DataTransformedEvent(
                transformation_id="transform-1",
                input_count=100,
                output_count=95,
                transformation_stats={"errors": 5},
                correlation_id=correlation_id
            )
            
            await event_store.append_event(transformed_event)
            await event_bus.publish(transformed_event)
            
            # 4. Migrate users
            user_events = []
            for i in range(10):
                user_event = UserMigratedEvent(
                    user_id=f"user-{i}",
                    username=f"testuser{i}",
                    source_data={"id": i},
                    target_data={"id": f"uuid-{i}"},
                    correlation_id=correlation_id
                )
                user_events.append(user_event)
            
            await event_store.append_events(user_events)
            await event_bus.publish_batch(user_events)
            
            # 5. Complete migration
            completed_event = MigrationCompletedEvent(
                migration_id=migration_id,
                statistics={"users": 10, "admins": 0},
                duration_seconds=120.0,
                correlation_id=correlation_id
            )
            
            await event_store.append_event(completed_event)
            await event_bus.publish(completed_event)
            
            # Give time for async processing
            await asyncio.sleep(0.2)
            
            # Verify results
            stats = migration_handler.get_migration_stats(migration_id)
            assert stats["status"] == "completed"
            assert stats["extracted_count"] == 100
            assert stats["transformed_count"] == 95
            assert stats["user_count"] == 10
            
            user_migrations = migration_handler.get_user_migrations()
            assert len(user_migrations) == 10
            
            # Test event replay
            replay = EventReplay(event_store)
            replay_handler = MigrationEventHandler()
            replay.add_handler(replay_handler)
            
            result = await replay.replay_events_by_correlation_id(correlation_id)
            
            assert result.processed_events >= 13  # All events
            assert result.failed_events == 0
            
            replay_stats = replay_handler.get_migration_stats(migration_id)
            assert replay_stats["status"] == "completed"
            assert replay_stats["user_count"] == 10
        
        finally:
            await event_bus.stop()
            await event_store.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])