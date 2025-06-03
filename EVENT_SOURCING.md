# Event Sourcing System Documentation

## Overview

The Event Sourcing system provides a comprehensive solution for capturing, storing, and replaying all changes in the migration system. This implementation follows CQRS (Command Query Responsibility Segregation) patterns and uses Apache Kafka for event streaming.

## Architecture

### Core Components

1. **Event Store**: Persistent storage for all events
2. **Event Bus**: Message broker for real-time event distribution
3. **Event Handlers**: Process events and update read models
4. **Event Replay**: Reconstruct system state from events

### Event Flow

```
[Command] → [Event] → [Event Store] → [Event Bus] → [Event Handlers] → [Read Models]
                           ↓
                    [Event Replay] ← [Event Store]
```

## Event Types

### Migration Events

- `MIGRATION_STARTED`: Migration process initiated
- `MIGRATION_COMPLETED`: Migration completed successfully
- `MIGRATION_FAILED`: Migration failed with errors

### Data Processing Events

- `DATA_EXTRACTED`: Data extracted from source system
- `DATA_TRANSFORMED`: Data transformation completed
- `DATA_VALIDATED`: Data validation completed
- `DATA_IMPORTED`: Data imported to target system

### Entity Migration Events

- `USER_MIGRATED`: User successfully migrated
- `ADMIN_MIGRATED`: Admin successfully migrated

### System Events

- `ROLLBACK_INITIATED`: Rollback process started
- `ROLLBACK_COMPLETED`: Rollback process completed
- `SCHEMA_COMPATIBILITY_CHECKED`: Schema compatibility verified
- `UUID_TRANSFORMED`: UUID format transformation
- `DATETIME_CONVERTED`: Datetime format conversion

## Event Structure

### Base Event

```python
class BaseEvent(BaseModel):
    event_id: UUID                    # Unique event identifier
    event_type: EventType            # Type of event
    aggregate_id: str                # ID of the aggregate
    aggregate_type: str              # Type of aggregate
    event_version: int               # Event version for ordering
    timestamp: datetime              # When the event occurred
    correlation_id: Optional[UUID]   # Links related events
    causation_id: Optional[UUID]     # What caused this event
    metadata: Dict[str, Any]         # Additional metadata
    data: Dict[str, Any]             # Event payload
```

### Event Metadata

Events include rich metadata for traceability:

- **Correlation ID**: Groups related events across services
- **Causation ID**: Links cause-and-effect relationships
- **Timestamp**: Precise timing information
- **Version**: Ensures proper event ordering

## Event Store

### Features

- **Persistent Storage**: PostgreSQL backend for durability
- **Efficient Querying**: Optimized indexes for common queries
- **Atomic Operations**: ACID compliance for data integrity
- **Scalable Design**: Supports high-throughput scenarios

### Storage Backends

#### PostgreSQL (Production)

```python
event_store_config = EventStoreConfig(
    database_url="postgresql+asyncpg://user:pass@host:5432/events"
)
event_store = EventStore(event_store_config)
```

#### In-Memory (Testing)

```python
event_store = EventStore(use_memory=True)
```

### Usage Examples

#### Storing Events

```python
# Single event
event = MigrationStartedEvent(
    migration_id="migration-123",
    source_config={"host": "source.example.com"},
    target_config={"host": "target.example.com"}
)
await event_store.append_event(event)

# Batch events
events = [event1, event2, event3]
await event_store.append_events(events)
```

#### Querying Events

```python
# Get events for an aggregate
events = await event_store.get_events("migration-123")

# Get events by type
migration_events = await event_store.get_events_by_type(
    EventType.MIGRATION_STARTED
)

# Get events by correlation ID
related_events = await event_store.get_events_by_correlation_id(
    correlation_id
)

# Stream all events
async for event in event_store.get_all_events():
    process_event(event)
```

## Event Bus

### Apache Kafka Integration

The event bus uses Apache Kafka for reliable, scalable event streaming:

- **Topic-per-Event-Type**: Each event type gets its own topic
- **Partitioning**: Events distributed across partitions for scalability
- **Consumer Groups**: Multiple handlers can process events independently
- **Retry Logic**: Automatic retry with exponential backoff
- **Dead Letter Queue**: Failed events sent to DLQ for analysis

### Configuration

```python
event_bus_config = EventBusConfig(
    kafka_bootstrap_servers="localhost:9092",
    topic_prefix="migration",
    retry_attempts=3,
    retry_delay=1.0,
    enable_dead_letter_queue=True
)
event_bus = EventBus(event_bus_config)
```

### Publishing Events

```python
# Single event
await event_bus.publish(event)

# Batch events
await event_bus.publish_batch(events)
```

### Subscribing to Events

```python
async def handle_migration_events(event: BaseEvent):
    if event.event_type == EventType.MIGRATION_STARTED:
        print(f"Migration {event.aggregate_id} started")

await event_bus.subscribe(
    [EventType.MIGRATION_STARTED, EventType.MIGRATION_COMPLETED],
    handle_migration_events,
    consumer_group="migration-processor"
)
```

## Event Handlers

### Migration Event Handler

Tracks migration progress and statistics:

```python
handler = MigrationEventHandler()

# Get migration statistics
stats = handler.get_migration_stats("migration-123")
# Returns: {
#     "status": "completed",
#     "started_at": "2023-01-01T10:00:00Z",
#     "user_count": 1000,
#     "admin_count": 5,
#     "extracted_count": 1005,
#     "transformed_count": 1005,
#     "imported_count": 1005
# }

# Get user migrations
users = handler.get_user_migrations()
# Returns: {"user-1": {"username": "john", "migrated_at": "..."}, ...}
```

### Notification Event Handler

Sends notifications for important events:

```python
notification_service = MyNotificationService()
handler = NotificationEventHandler(notification_service)

# Automatically sends notifications for:
# - Migration completed
# - Migration failed
# - Rollback initiated
# - Rollback completed
```

### Audit Event Handler

Logs all events for compliance and debugging:

```python
audit_logger = logging.getLogger("audit")
handler = AuditEventHandler(audit_logger)

# Logs all events with structured format
```

### Custom Event Handlers

```python
class CustomEventHandler(EventHandler):
    def can_handle(self, event_type: EventType) -> bool:
        return event_type == EventType.USER_MIGRATED
    
    async def handle(self, event: BaseEvent) -> None:
        # Custom processing logic
        user_id = event.aggregate_id
        username = event.data["username"]
        await self.update_user_index(user_id, username)
```

## Event Replay

### Features

- **State Reconstruction**: Rebuild system state from events
- **Debugging**: Replay events to understand system behavior
- **Testing**: Verify event sequences and handlers
- **Recovery**: Restore system after failures

### Replay Types

#### All Events

```python
replay = EventReplay(event_store)
replay.add_handler(migration_handler)

result = await replay.replay_all_events(
    from_timestamp=datetime(2023, 1, 1),
    to_timestamp=datetime(2023, 1, 31)
)
```

#### Aggregate Events

```python
result = await replay.replay_aggregate_events(
    aggregate_id="migration-123",
    from_version=1,
    to_version=10
)
```

#### Events by Type

```python
result = await replay.replay_events_by_type(
    event_types=[EventType.USER_MIGRATED, EventType.ADMIN_MIGRATED]
)
```

#### Events by Correlation

```python
result = await replay.replay_events_by_correlation_id(correlation_id)
```

### Replay Configuration

```python
config = EventReplayConfig(
    batch_size=100,              # Events per batch
    delay_between_batches=0.1,   # Delay in seconds
    stop_on_error=False,         # Continue on handler errors
    max_retries=3,               # Retry failed handlers
    retry_delay=1.0              # Retry delay in seconds
)
replay = EventReplay(event_store, config)
```

### Replay Results

```python
result = await replay.replay_all_events()

print(f"Total events: {result.total_events}")
print(f"Processed: {result.processed_events}")
print(f"Failed: {result.failed_events}")
print(f"Success rate: {result.success_rate}%")
print(f"Duration: {result.duration} seconds")

# Check for errors
for error in result.errors:
    print(f"Error in {error['event_id']}: {error['error']}")
```

### Snapshots

Create point-in-time snapshots of aggregate state:

```python
snapshot = await replay.create_snapshot(
    aggregate_id="migration-123",
    up_to_version=50
)

# Snapshot contains:
# - aggregate_id
# - version
# - snapshot_time
# - replay_result
# - state (from handlers)
```

### Sequence Validation

Verify that events follow expected patterns:

```python
expected_sequence = [
    EventType.MIGRATION_STARTED,
    EventType.DATA_EXTRACTED,
    EventType.DATA_TRANSFORMED,
    EventType.DATA_VALIDATED,
    EventType.DATA_IMPORTED,
    EventType.MIGRATION_COMPLETED
]

validation = await replay.validate_event_sequence(
    aggregate_id="migration-123",
    expected_sequence=expected_sequence
)

if not validation["is_valid"]:
    print("Sequence validation failed:")
    for diff in validation["differences"]:
        print(f"Position {diff['position']}: expected {diff['expected']}, got {diff['actual']}")
```

## Services

### Event Service (Port 8006)

REST API for event management:

#### Endpoints

- `POST /events` - Publish single event
- `POST /events/batch` - Publish multiple events
- `GET /events` - Query events
- `GET /events/stats` - Get event statistics
- `GET /migrations/{id}/stats` - Get migration statistics

#### Example Usage

```bash
# Publish an event
curl -X POST http://localhost:8006/events \
  -H "Content-Type: application/json" \
  -d '{
    "event_type": "migration.started",
    "aggregate_id": "migration-123",
    "aggregate_type": "migration",
    "data": {
      "source_config": {"host": "source.example.com"},
      "target_config": {"host": "target.example.com"}
    }
  }'

# Query events
curl "http://localhost:8006/events?aggregate_id=migration-123"

# Get statistics
curl "http://localhost:8006/events/stats"
```

### Replay Service (Port 8007)

REST API for event replay operations:

#### Endpoints

- `POST /replay` - Start replay operation
- `GET /replay/{id}` - Get replay result
- `GET /replay` - List all replays
- `POST /snapshot` - Create aggregate snapshot
- `POST /validate-sequence` - Validate event sequence
- `GET /events/stream` - Stream events (SSE)

#### Example Usage

```bash
# Start replay
curl -X POST http://localhost:8007/replay \
  -H "Content-Type: application/json" \
  -d '{
    "replay_type": "aggregate",
    "aggregate_id": "migration-123"
  }'

# Get replay result
curl "http://localhost:8007/replay/replay_20231201_120000_0"

# Create snapshot
curl -X POST http://localhost:8007/snapshot \
  -H "Content-Type: application/json" \
  -d '{
    "aggregate_id": "migration-123",
    "up_to_version": 50
  }'

# Stream events
curl "http://localhost:8007/events/stream?event_types=migration.started,migration.completed"
```

## Deployment

### Docker Compose

The system includes a complete Docker Compose setup:

```bash
# Start Kafka infrastructure
docker-compose -f docker-compose.kafka.yml up -d

# Start event services
docker-compose up event-service replay-service
```

### Services Included

- **Zookeeper**: Kafka coordination
- **Kafka**: Message broker
- **Kafka UI**: Web interface for Kafka management
- **Schema Registry**: Schema management (optional)
- **PostgreSQL**: Event store database
- **Event Service**: Event management API
- **Replay Service**: Event replay API

### Environment Variables

```bash
# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS=kafka:29092

# Database configuration
DATABASE_URL=postgresql+asyncpg://event_user:event_password@event-store-db:5432/event_store

# Service configuration
LOG_LEVEL=INFO
```

## Monitoring and Observability

### Kafka UI

Access Kafka UI at http://localhost:8080 to monitor:

- Topics and partitions
- Consumer groups and lag
- Message throughput
- Broker health

### Event Statistics

The Event Service provides comprehensive statistics:

```python
# Get overall statistics
stats = await get_event_stats()
# Returns:
# {
#     "total_events": 10000,
#     "events_by_type": {
#         "migration.started": 100,
#         "user.migrated": 8000,
#         "migration.completed": 95
#     },
#     "events_by_aggregate_type": {
#         "migration": 200,
#         "user": 8000,
#         "admin": 50
#     },
#     "latest_event_timestamp": "2023-12-01T12:00:00Z"
# }
```

### Health Checks

All services include health check endpoints:

```bash
curl http://localhost:8006/health  # Event Service
curl http://localhost:8007/health  # Replay Service
```

## Best Practices

### Event Design

1. **Immutable Events**: Never modify events after creation
2. **Rich Context**: Include correlation and causation IDs
3. **Meaningful Names**: Use descriptive event type names
4. **Versioning**: Plan for event schema evolution
5. **Idempotency**: Ensure handlers can process events multiple times

### Performance

1. **Batch Operations**: Use batch APIs for multiple events
2. **Async Processing**: Leverage async/await throughout
3. **Connection Pooling**: Configure appropriate pool sizes
4. **Indexing**: Ensure proper database indexes
5. **Partitioning**: Use Kafka partitioning for scalability

### Error Handling

1. **Retry Logic**: Implement exponential backoff
2. **Dead Letter Queues**: Handle permanently failed events
3. **Circuit Breakers**: Protect against cascading failures
4. **Monitoring**: Track error rates and patterns
5. **Alerting**: Set up alerts for critical failures

### Security

1. **Authentication**: Secure service-to-service communication
2. **Authorization**: Implement proper access controls
3. **Encryption**: Use TLS for data in transit
4. **Audit Logging**: Log all access and modifications
5. **Data Privacy**: Handle sensitive data appropriately

## Troubleshooting

### Common Issues

#### Kafka Connection Errors

```bash
# Check Kafka status
docker-compose -f docker-compose.kafka.yml ps

# View Kafka logs
docker-compose -f docker-compose.kafka.yml logs kafka

# Test connectivity
docker exec -it migration-kafka kafka-topics --bootstrap-server localhost:9092 --list
```

#### Database Connection Issues

```bash
# Check database status
docker-compose -f docker-compose.kafka.yml ps event-store-db

# Test database connection
docker exec -it migration-event-store-db psql -U event_user -d event_store -c "SELECT COUNT(*) FROM events;"
```

#### Event Processing Delays

1. Check consumer lag in Kafka UI
2. Monitor handler performance
3. Verify database performance
4. Check for error patterns

#### Memory Issues

1. Adjust batch sizes in replay operations
2. Monitor JVM heap usage (Kafka)
3. Optimize database queries
4. Use streaming for large datasets

### Debugging

#### Event Tracing

Use correlation IDs to trace event flows:

```python
# Find all events in a migration
events = await event_store.get_events_by_correlation_id(correlation_id)
for event in events:
    print(f"{event.timestamp}: {event.event_type} - {event.aggregate_id}")
```

#### Handler Testing

Test handlers in isolation:

```python
handler = MigrationEventHandler()
test_event = MigrationStartedEvent(...)
await handler.handle(test_event)
assert handler.get_migration_stats("test-id")["status"] == "started"
```

#### Replay Debugging

Use replay to understand system behavior:

```python
# Replay with detailed logging
replay = EventReplay(event_store)
replay.add_handler(DebugEventHandler())
result = await replay.replay_aggregate_events("problematic-migration")
```

## Migration from Legacy System

### Event Backfill

To migrate from a legacy system:

1. **Extract Historical Data**: Export existing data
2. **Generate Events**: Create events for historical changes
3. **Backfill Store**: Import events with historical timestamps
4. **Verify Consistency**: Compare final state with legacy system

```python
# Example backfill process
async def backfill_user_migrations(legacy_users):
    events = []
    for user in legacy_users:
        event = UserMigratedEvent(
            user_id=user.id,
            username=user.username,
            source_data=user.to_dict(),
            target_data=user.to_dict()
        )
        # Set historical timestamp
        event.timestamp = user.migrated_at
        events.append(event)
    
    await event_store.append_events(events)
```

### Gradual Migration

1. **Dual Write**: Write to both legacy and event systems
2. **Read from Events**: Gradually switch reads to event-based data
3. **Verify Consistency**: Compare results between systems
4. **Switch Over**: Disable legacy system writes
5. **Cleanup**: Remove legacy system dependencies

## Future Enhancements

### Planned Features

1. **Event Encryption**: Encrypt sensitive event data
2. **Schema Registry**: Formal schema management
3. **Event Compaction**: Optimize storage for large event streams
4. **Multi-Region**: Support for geographically distributed deployments
5. **Advanced Analytics**: Real-time event analytics and insights

### Integration Opportunities

1. **Monitoring**: Integration with Prometheus/Grafana
2. **Alerting**: Integration with PagerDuty/Slack
3. **Tracing**: Integration with Jaeger/Zipkin
4. **Metrics**: Custom business metrics and dashboards
5. **Machine Learning**: Event pattern analysis and prediction

---

This event sourcing system provides a robust foundation for the migration system, enabling comprehensive audit trails, system recovery, and real-time monitoring of all migration activities.