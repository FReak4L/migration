"""
Event Replay System

This module provides functionality to replay events from the event store,
allowing for system recovery, debugging, and state reconstruction.
"""

import logging
from datetime import datetime
from typing import List, Optional, Dict, Any, Callable, AsyncIterator
from uuid import UUID

from .event_store import EventStore
from .event_models import BaseEvent, EventType
from .event_handlers import EventHandler


logger = logging.getLogger(__name__)


class EventReplayConfig:
    """Configuration for event replay."""
    
    def __init__(
        self,
        batch_size: int = 100,
        delay_between_batches: float = 0.1,
        stop_on_error: bool = False,
        max_retries: int = 3,
        retry_delay: float = 1.0
    ):
        self.batch_size = batch_size
        self.delay_between_batches = delay_between_batches
        self.stop_on_error = stop_on_error
        self.max_retries = max_retries
        self.retry_delay = retry_delay


class ReplayResult:
    """Result of an event replay operation."""
    
    def __init__(self):
        self.total_events = 0
        self.processed_events = 0
        self.failed_events = 0
        self.errors: List[Dict[str, Any]] = []
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
    
    @property
    def duration(self) -> Optional[float]:
        """Get the duration of the replay in seconds."""
        if self.start_time and self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return None
    
    @property
    def success_rate(self) -> float:
        """Get the success rate as a percentage."""
        if self.total_events == 0:
            return 0.0
        return (self.processed_events / self.total_events) * 100
    
    def add_error(self, event: BaseEvent, error: Exception) -> None:
        """Add an error to the result."""
        self.errors.append({
            'event_id': str(event.event_id),
            'event_type': event.event_type.value,
            'aggregate_id': event.aggregate_id,
            'timestamp': event.timestamp.isoformat(),
            'error': str(error),
            'error_type': type(error).__name__
        })
        self.failed_events += 1
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        return {
            'total_events': self.total_events,
            'processed_events': self.processed_events,
            'failed_events': self.failed_events,
            'success_rate': self.success_rate,
            'duration': self.duration,
            'start_time': self.start_time.isoformat() if self.start_time else None,
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'errors': self.errors
        }


class EventReplay:
    """Event replay system for reconstructing state and debugging."""
    
    def __init__(self, event_store: EventStore, config: Optional[EventReplayConfig] = None):
        self.event_store = event_store
        self.config = config or EventReplayConfig()
        self._handlers: List[EventHandler] = []
    
    def add_handler(self, handler: EventHandler) -> None:
        """Add an event handler for replay."""
        self._handlers.append(handler)
    
    def remove_handler(self, handler: EventHandler) -> None:
        """Remove an event handler."""
        if handler in self._handlers:
            self._handlers.remove(handler)
    
    async def replay_all_events(
        self,
        from_timestamp: Optional[datetime] = None,
        to_timestamp: Optional[datetime] = None,
        event_filter: Optional[Callable[[BaseEvent], bool]] = None
    ) -> ReplayResult:
        """Replay all events in the store."""
        result = ReplayResult()
        result.start_time = datetime.utcnow()
        
        try:
            logger.info("Starting event replay for all events")
            
            # Get all events from the store
            events_iterator = self.event_store.get_all_events(
                from_timestamp=from_timestamp,
                to_timestamp=to_timestamp
            )
            
            await self._process_events(events_iterator, event_filter, result)
            
        except Exception as e:
            logger.error(f"Error during event replay: {e}")
            result.add_error(BaseEvent(event_type=EventType.MIGRATION_FAILED, aggregate_id="replay", aggregate_type="system"), e)
        
        finally:
            result.end_time = datetime.utcnow()
            logger.info(f"Event replay completed: {result.processed_events}/{result.total_events} events processed")
        
        return result
    
    async def replay_aggregate_events(
        self,
        aggregate_id: str,
        from_version: Optional[int] = None,
        to_version: Optional[int] = None,
        event_filter: Optional[Callable[[BaseEvent], bool]] = None
    ) -> ReplayResult:
        """Replay events for a specific aggregate."""
        result = ReplayResult()
        result.start_time = datetime.utcnow()
        
        try:
            logger.info(f"Starting event replay for aggregate {aggregate_id}")
            
            # Get events for the specific aggregate
            events = await self.event_store.get_events(
                aggregate_id=aggregate_id,
                from_version=from_version,
                to_version=to_version
            )
            
            # Convert list to async iterator
            async def events_iterator():
                for event in events:
                    yield event
            
            await self._process_events(events_iterator(), event_filter, result)
            
        except Exception as e:
            logger.error(f"Error during aggregate event replay: {e}")
            result.add_error(BaseEvent(event_type=EventType.MIGRATION_FAILED, aggregate_id=aggregate_id, aggregate_type="aggregate"), e)
        
        finally:
            result.end_time = datetime.utcnow()
            logger.info(f"Aggregate event replay completed: {result.processed_events}/{result.total_events} events processed")
        
        return result
    
    async def replay_events_by_type(
        self,
        event_types: List[EventType],
        from_timestamp: Optional[datetime] = None,
        to_timestamp: Optional[datetime] = None,
        event_filter: Optional[Callable[[BaseEvent], bool]] = None
    ) -> ReplayResult:
        """Replay events of specific types."""
        result = ReplayResult()
        result.start_time = datetime.utcnow()
        
        try:
            logger.info(f"Starting event replay for event types: {[et.value for et in event_types]}")
            
            # Get events for each type and combine them
            all_events = []
            for event_type in event_types:
                events = await self.event_store.get_events_by_type(
                    event_type=event_type,
                    from_timestamp=from_timestamp,
                    to_timestamp=to_timestamp
                )
                all_events.extend(events)
            
            # Sort events by timestamp
            all_events.sort(key=lambda e: e.timestamp)
            
            # Convert list to async iterator
            async def events_iterator():
                for event in all_events:
                    yield event
            
            await self._process_events(events_iterator(), event_filter, result)
            
        except Exception as e:
            logger.error(f"Error during event type replay: {e}")
            result.add_error(BaseEvent(event_type=EventType.MIGRATION_FAILED, aggregate_id="replay", aggregate_type="system"), e)
        
        finally:
            result.end_time = datetime.utcnow()
            logger.info(f"Event type replay completed: {result.processed_events}/{result.total_events} events processed")
        
        return result
    
    async def replay_events_by_correlation_id(
        self,
        correlation_id: UUID,
        event_filter: Optional[Callable[[BaseEvent], bool]] = None
    ) -> ReplayResult:
        """Replay events with a specific correlation ID."""
        result = ReplayResult()
        result.start_time = datetime.utcnow()
        
        try:
            logger.info(f"Starting event replay for correlation ID {correlation_id}")
            
            # Get events by correlation ID
            events = await self.event_store.get_events_by_correlation_id(correlation_id)
            
            # Sort events by timestamp
            events.sort(key=lambda e: e.timestamp)
            
            # Convert list to async iterator
            async def events_iterator():
                for event in events:
                    yield event
            
            await self._process_events(events_iterator(), event_filter, result)
            
        except Exception as e:
            logger.error(f"Error during correlation ID event replay: {e}")
            result.add_error(BaseEvent(event_type=EventType.MIGRATION_FAILED, aggregate_id="replay", aggregate_type="system"), e)
        
        finally:
            result.end_time = datetime.utcnow()
            logger.info(f"Correlation ID event replay completed: {result.processed_events}/{result.total_events} events processed")
        
        return result
    
    async def _process_events(
        self,
        events_iterator: AsyncIterator[BaseEvent],
        event_filter: Optional[Callable[[BaseEvent], bool]],
        result: ReplayResult
    ) -> None:
        """Process events through handlers."""
        import asyncio
        
        batch = []
        
        async for event in events_iterator:
            result.total_events += 1
            
            # Apply filter if provided
            if event_filter and not event_filter(event):
                continue
            
            batch.append(event)
            
            # Process batch when it reaches the configured size
            if len(batch) >= self.config.batch_size:
                await self._process_batch(batch, result)
                batch = []
                
                # Add delay between batches if configured
                if self.config.delay_between_batches > 0:
                    await asyncio.sleep(self.config.delay_between_batches)
        
        # Process remaining events in the last batch
        if batch:
            await self._process_batch(batch, result)
    
    async def _process_batch(self, events: List[BaseEvent], result: ReplayResult) -> None:
        """Process a batch of events."""
        for event in events:
            try:
                await self._process_single_event(event)
                result.processed_events += 1
                
            except Exception as e:
                logger.error(f"Error processing event {event.event_id}: {e}")
                result.add_error(event, e)
                
                if self.config.stop_on_error:
                    raise
    
    async def _process_single_event(self, event: BaseEvent) -> None:
        """Process a single event through all applicable handlers."""
        import asyncio
        
        for handler in self._handlers:
            if handler.can_handle(event.event_type):
                # Retry logic for handler execution
                for attempt in range(self.config.max_retries + 1):
                    try:
                        await handler.handle(event)
                        break
                    
                    except Exception as e:
                        if attempt < self.config.max_retries:
                            logger.warning(
                                f"Handler {type(handler).__name__} failed for event {event.event_id} "
                                f"(attempt {attempt + 1}/{self.config.max_retries + 1}): {e}"
                            )
                            await asyncio.sleep(self.config.retry_delay * (attempt + 1))
                        else:
                            logger.error(
                                f"Handler {type(handler).__name__} failed for event {event.event_id} "
                                f"after {self.config.max_retries + 1} attempts: {e}"
                            )
                            raise
    
    async def create_snapshot(
        self,
        aggregate_id: str,
        up_to_version: Optional[int] = None
    ) -> Dict[str, Any]:
        """Create a snapshot of an aggregate's state by replaying its events."""
        from .event_handlers import MigrationEventHandler
        
        # Create a temporary handler to capture state
        handler = MigrationEventHandler()
        
        # Temporarily add the handler
        self.add_handler(handler)
        
        try:
            # Replay events for the aggregate
            result = await self.replay_aggregate_events(
                aggregate_id=aggregate_id,
                to_version=up_to_version
            )
            
            # Get the final state from the handler
            snapshot = {
                'aggregate_id': aggregate_id,
                'version': up_to_version,
                'snapshot_time': datetime.utcnow().isoformat(),
                'replay_result': result.to_dict(),
                'state': handler.get_migration_stats(aggregate_id)
            }
            
            return snapshot
        
        finally:
            # Remove the temporary handler
            self.remove_handler(handler)
    
    async def validate_event_sequence(
        self,
        aggregate_id: str,
        expected_sequence: List[EventType]
    ) -> Dict[str, Any]:
        """Validate that events for an aggregate follow an expected sequence."""
        events = await self.event_store.get_events(aggregate_id)
        
        actual_sequence = [event.event_type for event in events]
        
        validation_result = {
            'aggregate_id': aggregate_id,
            'expected_sequence': [et.value for et in expected_sequence],
            'actual_sequence': [et.value for et in actual_sequence],
            'is_valid': actual_sequence == expected_sequence,
            'differences': []
        }
        
        # Find differences
        max_length = max(len(expected_sequence), len(actual_sequence))
        for i in range(max_length):
            expected = expected_sequence[i] if i < len(expected_sequence) else None
            actual = actual_sequence[i] if i < len(actual_sequence) else None
            
            if expected != actual:
                validation_result['differences'].append({
                    'position': i,
                    'expected': expected.value if expected else None,
                    'actual': actual.value if actual else None
                })
        
        return validation_result