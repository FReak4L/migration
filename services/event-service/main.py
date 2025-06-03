"""
Event Service

This service provides REST API endpoints for event sourcing functionality,
including event publishing, querying, and management.
"""

import asyncio
import logging
from datetime import datetime
from typing import List, Optional, Dict, Any
from uuid import UUID
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Depends, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import uvicorn

from events import (
    EventStore, EventBus, EventStoreConfig, EventBusConfig,
    BaseEvent, EventType, MigrationEventHandler, NotificationEventHandler,
    AuditEventHandler
)


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Pydantic models for API
class EventRequest(BaseModel):
    event_type: EventType
    aggregate_id: str
    aggregate_type: str
    data: Dict[str, Any] = Field(default_factory=dict)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    correlation_id: Optional[UUID] = None
    causation_id: Optional[UUID] = None


class EventResponse(BaseModel):
    event_id: UUID
    event_type: EventType
    aggregate_id: str
    aggregate_type: str
    event_version: int
    timestamp: datetime
    correlation_id: Optional[UUID]
    causation_id: Optional[UUID]
    metadata: Dict[str, Any]
    data: Dict[str, Any]


class EventQueryRequest(BaseModel):
    aggregate_id: Optional[str] = None
    event_types: Optional[List[EventType]] = None
    from_timestamp: Optional[datetime] = None
    to_timestamp: Optional[datetime] = None
    correlation_id: Optional[UUID] = None
    limit: Optional[int] = 100


class EventStatsResponse(BaseModel):
    total_events: int
    events_by_type: Dict[str, int]
    events_by_aggregate_type: Dict[str, int]
    latest_event_timestamp: Optional[datetime]


# Global instances
event_store: Optional[EventStore] = None
event_bus: Optional[EventBus] = None
migration_handler: Optional[MigrationEventHandler] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan."""
    global event_store, event_bus, migration_handler
    
    # Startup
    logger.info("Starting Event Service...")
    
    # Initialize event store
    event_store_config = EventStoreConfig(
        database_url="postgresql+asyncpg://event_user:event_password@event-store-db:5432/event_store"
    )
    event_store = EventStore(event_store_config)
    await event_store.initialize()
    
    # Initialize event bus
    event_bus_config = EventBusConfig(
        kafka_bootstrap_servers="kafka:29092"
    )
    event_bus = EventBus(event_bus_config)
    await event_bus.start()
    
    # Initialize event handlers
    migration_handler = MigrationEventHandler()
    notification_handler = NotificationEventHandler()
    audit_handler = AuditEventHandler()
    
    # Subscribe handlers to event bus
    await event_bus.subscribe(
        [et for et in EventType],
        migration_handler.handle,
        "migration-handler"
    )
    
    await event_bus.subscribe(
        [EventType.MIGRATION_COMPLETED, EventType.MIGRATION_FAILED, 
         EventType.ROLLBACK_INITIATED, EventType.ROLLBACK_COMPLETED],
        notification_handler.handle,
        "notification-handler"
    )
    
    await event_bus.subscribe(
        [et for et in EventType],
        audit_handler.handle,
        "audit-handler"
    )
    
    logger.info("Event Service started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Event Service...")
    
    if event_bus:
        await event_bus.stop()
    
    if event_store:
        await event_store.close()
    
    logger.info("Event Service shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="Migration Event Service",
    description="Event sourcing service for migration system",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_event_store() -> EventStore:
    """Dependency to get event store instance."""
    if event_store is None:
        raise HTTPException(status_code=503, detail="Event store not available")
    return event_store


def get_event_bus() -> EventBus:
    """Dependency to get event bus instance."""
    if event_bus is None:
        raise HTTPException(status_code=503, detail="Event bus not available")
    return event_bus


def get_migration_handler() -> MigrationEventHandler:
    """Dependency to get migration handler instance."""
    if migration_handler is None:
        raise HTTPException(status_code=503, detail="Migration handler not available")
    return migration_handler


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "event-service"
    }


@app.post("/events", response_model=EventResponse)
async def publish_event(
    event_request: EventRequest,
    background_tasks: BackgroundTasks,
    store: EventStore = Depends(get_event_store),
    bus: EventBus = Depends(get_event_bus)
):
    """Publish a new event."""
    try:
        # Create event
        event = BaseEvent(
            event_type=event_request.event_type,
            aggregate_id=event_request.aggregate_id,
            aggregate_type=event_request.aggregate_type,
            data=event_request.data,
            metadata=event_request.metadata,
            correlation_id=event_request.correlation_id,
            causation_id=event_request.causation_id
        )
        
        # Store event
        await store.append_event(event)
        
        # Publish event to bus (in background)
        background_tasks.add_task(bus.publish, event)
        
        return EventResponse(
            event_id=event.event_id,
            event_type=event.event_type,
            aggregate_id=event.aggregate_id,
            aggregate_type=event.aggregate_type,
            event_version=event.event_version,
            timestamp=event.timestamp,
            correlation_id=event.correlation_id,
            causation_id=event.causation_id,
            metadata=event.metadata,
            data=event.data
        )
    
    except Exception as e:
        logger.error(f"Error publishing event: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/events/batch")
async def publish_events_batch(
    events_request: List[EventRequest],
    background_tasks: BackgroundTasks,
    store: EventStore = Depends(get_event_store),
    bus: EventBus = Depends(get_event_bus)
):
    """Publish multiple events in a batch."""
    try:
        # Create events
        events = []
        for event_request in events_request:
            event = BaseEvent(
                event_type=event_request.event_type,
                aggregate_id=event_request.aggregate_id,
                aggregate_type=event_request.aggregate_type,
                data=event_request.data,
                metadata=event_request.metadata,
                correlation_id=event_request.correlation_id,
                causation_id=event_request.causation_id
            )
            events.append(event)
        
        # Store events
        await store.append_events(events)
        
        # Publish events to bus (in background)
        background_tasks.add_task(bus.publish_batch, events)
        
        return {
            "message": f"Published {len(events)} events successfully",
            "event_ids": [str(event.event_id) for event in events]
        }
    
    except Exception as e:
        logger.error(f"Error publishing batch events: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/events", response_model=List[EventResponse])
async def query_events(
    query: EventQueryRequest = Depends(),
    store: EventStore = Depends(get_event_store)
):
    """Query events based on criteria."""
    try:
        events = []
        
        if query.aggregate_id:
            # Get events for specific aggregate
            events = await store.get_events(query.aggregate_id)
        
        elif query.correlation_id:
            # Get events by correlation ID
            events = await store.get_events_by_correlation_id(query.correlation_id)
        
        elif query.event_types:
            # Get events by type
            for event_type in query.event_types:
                type_events = await store.get_events_by_type(
                    event_type,
                    query.from_timestamp,
                    query.to_timestamp
                )
                events.extend(type_events)
        
        else:
            # Get all events with optional filtering
            events_iter = store.get_all_events(
                query.from_timestamp,
                query.to_timestamp,
                query.limit
            )
            events = [event async for event in events_iter]
        
        # Apply limit if specified
        if query.limit and len(events) > query.limit:
            events = events[:query.limit]
        
        return [
            EventResponse(
                event_id=event.event_id,
                event_type=event.event_type,
                aggregate_id=event.aggregate_id,
                aggregate_type=event.aggregate_type,
                event_version=event.event_version,
                timestamp=event.timestamp,
                correlation_id=event.correlation_id,
                causation_id=event.causation_id,
                metadata=event.metadata,
                data=event.data
            )
            for event in events
        ]
    
    except Exception as e:
        logger.error(f"Error querying events: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/events/stats", response_model=EventStatsResponse)
async def get_event_stats(
    store: EventStore = Depends(get_event_store)
):
    """Get event statistics."""
    try:
        # Get all events to calculate stats
        events_iter = store.get_all_events()
        events = [event async for event in events_iter]
        
        # Calculate statistics
        total_events = len(events)
        events_by_type = {}
        events_by_aggregate_type = {}
        latest_timestamp = None
        
        for event in events:
            # Count by event type
            event_type_str = event.event_type.value
            events_by_type[event_type_str] = events_by_type.get(event_type_str, 0) + 1
            
            # Count by aggregate type
            aggregate_type = event.aggregate_type
            events_by_aggregate_type[aggregate_type] = events_by_aggregate_type.get(aggregate_type, 0) + 1
            
            # Track latest timestamp
            if latest_timestamp is None or event.timestamp > latest_timestamp:
                latest_timestamp = event.timestamp
        
        return EventStatsResponse(
            total_events=total_events,
            events_by_type=events_by_type,
            events_by_aggregate_type=events_by_aggregate_type,
            latest_event_timestamp=latest_timestamp
        )
    
    except Exception as e:
        logger.error(f"Error getting event stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/migrations/{migration_id}/stats")
async def get_migration_stats(
    migration_id: str,
    handler: MigrationEventHandler = Depends(get_migration_handler)
):
    """Get statistics for a specific migration."""
    try:
        stats = handler.get_migration_stats(migration_id)
        if not stats:
            raise HTTPException(status_code=404, detail="Migration not found")
        
        return stats
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting migration stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/migrations/stats")
async def get_all_migration_stats(
    handler: MigrationEventHandler = Depends(get_migration_handler)
):
    """Get statistics for all migrations."""
    try:
        return handler.get_all_migration_stats()
    
    except Exception as e:
        logger.error(f"Error getting all migration stats: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/users/migrations")
async def get_user_migrations(
    handler: MigrationEventHandler = Depends(get_migration_handler)
):
    """Get all user migrations."""
    try:
        return handler.get_user_migrations()
    
    except Exception as e:
        logger.error(f"Error getting user migrations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/admins/migrations")
async def get_admin_migrations(
    handler: MigrationEventHandler = Depends(get_migration_handler)
):
    """Get all admin migrations."""
    try:
        return handler.get_admin_migrations()
    
    except Exception as e:
        logger.error(f"Error getting admin migrations: {e}")
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8006,
        reload=True,
        log_level="info"
    )