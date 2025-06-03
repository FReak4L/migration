"""
Event Replay Service

This service provides REST API endpoints for event replay functionality,
including replay operations, snapshot creation, and sequence validation.
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
import httpx

from events import (
    EventStore, EventStoreConfig, EventReplay, EventReplayConfig,
    EventType, MigrationEventHandler, NotificationEventHandler,
    AuditEventHandler
)


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Pydantic models for API
class ReplayRequest(BaseModel):
    replay_type: str = Field(..., description="Type of replay: 'all', 'aggregate', 'type', 'correlation'")
    aggregate_id: Optional[str] = None
    event_types: Optional[List[EventType]] = None
    correlation_id: Optional[UUID] = None
    from_timestamp: Optional[datetime] = None
    to_timestamp: Optional[datetime] = None
    from_version: Optional[int] = None
    to_version: Optional[int] = None
    batch_size: int = 100
    delay_between_batches: float = 0.1
    stop_on_error: bool = False


class ReplayResponse(BaseModel):
    replay_id: str
    status: str
    message: str


class ReplayResultResponse(BaseModel):
    replay_id: str
    total_events: int
    processed_events: int
    failed_events: int
    success_rate: float
    duration: Optional[float]
    start_time: Optional[datetime]
    end_time: Optional[datetime]
    errors: List[Dict[str, Any]]


class SnapshotRequest(BaseModel):
    aggregate_id: str
    up_to_version: Optional[int] = None


class SequenceValidationRequest(BaseModel):
    aggregate_id: str
    expected_sequence: List[EventType]


# Global instances
event_store: Optional[EventStore] = None
event_replay: Optional[EventReplay] = None
active_replays: Dict[str, Dict[str, Any]] = {}


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan."""
    global event_store, event_replay
    
    # Startup
    logger.info("Starting Event Replay Service...")
    
    # Initialize event store
    event_store_config = EventStoreConfig(
        database_url="postgresql+asyncpg://event_user:event_password@event-store-db:5432/event_store"
    )
    event_store = EventStore(event_store_config)
    await event_store.initialize()
    
    # Initialize event replay
    replay_config = EventReplayConfig(
        batch_size=100,
        delay_between_batches=0.1,
        stop_on_error=False
    )
    event_replay = EventReplay(event_store, replay_config)
    
    logger.info("Event Replay Service started successfully")
    
    yield
    
    # Shutdown
    logger.info("Shutting down Event Replay Service...")
    
    if event_store:
        await event_store.close()
    
    logger.info("Event Replay Service shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="Migration Event Replay Service",
    description="Event replay service for migration system",
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


def get_event_replay() -> EventReplay:
    """Dependency to get event replay instance."""
    if event_replay is None:
        raise HTTPException(status_code=503, detail="Event replay not available")
    return event_replay


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "service": "replay-service"
    }


@app.post("/replay", response_model=ReplayResponse)
async def start_replay(
    request: ReplayRequest,
    background_tasks: BackgroundTasks,
    replay_service: EventReplay = Depends(get_event_replay)
):
    """Start an event replay operation."""
    try:
        replay_id = f"replay_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{len(active_replays)}"
        
        # Store replay info
        active_replays[replay_id] = {
            "status": "running",
            "request": request.dict(),
            "start_time": datetime.utcnow(),
            "result": None
        }
        
        # Start replay in background
        background_tasks.add_task(
            execute_replay,
            replay_id,
            request,
            replay_service
        )
        
        return ReplayResponse(
            replay_id=replay_id,
            status="started",
            message=f"Replay {replay_id} started successfully"
        )
    
    except Exception as e:
        logger.error(f"Error starting replay: {e}")
        raise HTTPException(status_code=500, detail=str(e))


async def execute_replay(
    replay_id: str,
    request: ReplayRequest,
    replay_service: EventReplay
):
    """Execute the replay operation."""
    try:
        # Update replay config if provided
        if hasattr(replay_service, 'config'):
            replay_service.config.batch_size = request.batch_size
            replay_service.config.delay_between_batches = request.delay_between_batches
            replay_service.config.stop_on_error = request.stop_on_error
        
        # Add handlers
        migration_handler = MigrationEventHandler()
        notification_handler = NotificationEventHandler()
        audit_handler = AuditEventHandler()
        
        replay_service.add_handler(migration_handler)
        replay_service.add_handler(notification_handler)
        replay_service.add_handler(audit_handler)
        
        # Execute replay based on type
        if request.replay_type == "all":
            result = await replay_service.replay_all_events(
                from_timestamp=request.from_timestamp,
                to_timestamp=request.to_timestamp
            )
        
        elif request.replay_type == "aggregate":
            if not request.aggregate_id:
                raise ValueError("aggregate_id is required for aggregate replay")
            
            result = await replay_service.replay_aggregate_events(
                aggregate_id=request.aggregate_id,
                from_version=request.from_version,
                to_version=request.to_version
            )
        
        elif request.replay_type == "type":
            if not request.event_types:
                raise ValueError("event_types is required for type replay")
            
            result = await replay_service.replay_events_by_type(
                event_types=request.event_types,
                from_timestamp=request.from_timestamp,
                to_timestamp=request.to_timestamp
            )
        
        elif request.replay_type == "correlation":
            if not request.correlation_id:
                raise ValueError("correlation_id is required for correlation replay")
            
            result = await replay_service.replay_events_by_correlation_id(
                correlation_id=request.correlation_id
            )
        
        else:
            raise ValueError(f"Invalid replay type: {request.replay_type}")
        
        # Update replay status
        active_replays[replay_id].update({
            "status": "completed",
            "end_time": datetime.utcnow(),
            "result": result.to_dict()
        })
        
        logger.info(f"Replay {replay_id} completed successfully")
    
    except Exception as e:
        logger.error(f"Error in replay {replay_id}: {e}")
        active_replays[replay_id].update({
            "status": "failed",
            "end_time": datetime.utcnow(),
            "error": str(e)
        })


@app.get("/replay/{replay_id}", response_model=ReplayResultResponse)
async def get_replay_result(replay_id: str):
    """Get the result of a replay operation."""
    try:
        if replay_id not in active_replays:
            raise HTTPException(status_code=404, detail="Replay not found")
        
        replay_info = active_replays[replay_id]
        
        if replay_info["status"] == "running":
            raise HTTPException(status_code=202, detail="Replay still running")
        
        if replay_info["status"] == "failed":
            raise HTTPException(status_code=500, detail=replay_info.get("error", "Replay failed"))
        
        result = replay_info["result"]
        return ReplayResultResponse(
            replay_id=replay_id,
            total_events=result["total_events"],
            processed_events=result["processed_events"],
            failed_events=result["failed_events"],
            success_rate=result["success_rate"],
            duration=result["duration"],
            start_time=datetime.fromisoformat(result["start_time"]) if result["start_time"] else None,
            end_time=datetime.fromisoformat(result["end_time"]) if result["end_time"] else None,
            errors=result["errors"]
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting replay result: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/replay")
async def list_replays():
    """List all replay operations."""
    try:
        replays = []
        for replay_id, info in active_replays.items():
            replays.append({
                "replay_id": replay_id,
                "status": info["status"],
                "start_time": info["start_time"].isoformat(),
                "end_time": info.get("end_time").isoformat() if info.get("end_time") else None,
                "replay_type": info["request"]["replay_type"]
            })
        
        return {"replays": replays}
    
    except Exception as e:
        logger.error(f"Error listing replays: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/snapshot")
async def create_snapshot(
    request: SnapshotRequest,
    replay_service: EventReplay = Depends(get_event_replay)
):
    """Create a snapshot of an aggregate's state."""
    try:
        snapshot = await replay_service.create_snapshot(
            aggregate_id=request.aggregate_id,
            up_to_version=request.up_to_version
        )
        
        return snapshot
    
    except Exception as e:
        logger.error(f"Error creating snapshot: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/validate-sequence")
async def validate_sequence(
    request: SequenceValidationRequest,
    replay_service: EventReplay = Depends(get_event_replay)
):
    """Validate event sequence for an aggregate."""
    try:
        validation_result = await replay_service.validate_event_sequence(
            aggregate_id=request.aggregate_id,
            expected_sequence=request.expected_sequence
        )
        
        return validation_result
    
    except Exception as e:
        logger.error(f"Error validating sequence: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/replay/{replay_id}")
async def cancel_replay(replay_id: str):
    """Cancel a running replay operation."""
    try:
        if replay_id not in active_replays:
            raise HTTPException(status_code=404, detail="Replay not found")
        
        replay_info = active_replays[replay_id]
        
        if replay_info["status"] != "running":
            raise HTTPException(status_code=400, detail="Replay is not running")
        
        # Mark as cancelled (actual cancellation would require more complex task management)
        active_replays[replay_id].update({
            "status": "cancelled",
            "end_time": datetime.utcnow()
        })
        
        return {"message": f"Replay {replay_id} cancelled"}
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error cancelling replay: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/events/stream")
async def stream_events(
    from_timestamp: Optional[datetime] = None,
    to_timestamp: Optional[datetime] = None,
    event_types: Optional[str] = None,
    store: EventStore = Depends(get_event_store)
):
    """Stream events in real-time (Server-Sent Events)."""
    from fastapi.responses import StreamingResponse
    import json
    
    async def event_generator():
        try:
            # Parse event types if provided
            filter_types = []
            if event_types:
                filter_types = [EventType(t.strip()) for t in event_types.split(",")]
            
            # Stream events
            async for event in store.get_all_events(from_timestamp, to_timestamp):
                # Apply event type filter
                if filter_types and event.event_type not in filter_types:
                    continue
                
                # Convert event to JSON
                event_data = {
                    "event_id": str(event.event_id),
                    "event_type": event.event_type.value,
                    "aggregate_id": event.aggregate_id,
                    "aggregate_type": event.aggregate_type,
                    "timestamp": event.timestamp.isoformat(),
                    "data": event.data
                }
                
                yield f"data: {json.dumps(event_data)}\n\n"
                
                # Small delay to prevent overwhelming the client
                await asyncio.sleep(0.01)
        
        except Exception as e:
            logger.error(f"Error in event stream: {e}")
            yield f"data: {json.dumps({'error': str(e)})}\n\n"
    
    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8007,
        reload=True,
        log_level="info"
    )