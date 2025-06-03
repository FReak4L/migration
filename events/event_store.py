"""
Event Store Implementation

This module provides the event store functionality for persisting and retrieving events.
Supports both in-memory and persistent storage backends.
"""

import json
import asyncio
from datetime import datetime
from typing import List, Optional, Dict, Any, AsyncIterator
from uuid import UUID
from abc import ABC, abstractmethod
from sqlalchemy import create_engine, Column, String, DateTime, Text, Integer, Index
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from pydantic import BaseModel

from .event_models import BaseEvent, EventType


Base = declarative_base()


class EventRecord(Base):
    """SQLAlchemy model for storing events."""
    
    __tablename__ = "events"
    
    event_id = Column(String(36), primary_key=True)
    event_type = Column(String(100), nullable=False)
    aggregate_id = Column(String(100), nullable=False)
    aggregate_type = Column(String(50), nullable=False)
    event_version = Column(Integer, nullable=False, default=1)
    timestamp = Column(DateTime, nullable=False)
    correlation_id = Column(String(36), nullable=True)
    causation_id = Column(String(36), nullable=True)
    event_metadata = Column(Text, nullable=False, default="{}")  # Renamed from 'metadata'
    event_data = Column(Text, nullable=False, default="{}")     # Renamed from 'data'
    
    # Indexes for efficient querying
    __table_args__ = (
        Index('idx_aggregate_id_version', 'aggregate_id', 'event_version'),
        Index('idx_event_type', 'event_type'),
        Index('idx_timestamp', 'timestamp'),
        Index('idx_correlation_id', 'correlation_id'),
    )


class EventStoreConfig(BaseModel):
    """Configuration for event store."""
    
    database_url: str = "sqlite+aiosqlite:///./events.db"
    max_connections: int = 10
    echo_sql: bool = False
    create_tables: bool = True


class IEventStore(ABC):
    """Interface for event store implementations."""
    
    @abstractmethod
    async def append_event(self, event: BaseEvent) -> None:
        """Append a single event to the store."""
        pass
    
    @abstractmethod
    async def append_events(self, events: List[BaseEvent]) -> None:
        """Append multiple events to the store."""
        pass
    
    @abstractmethod
    async def get_events(
        self, 
        aggregate_id: str, 
        from_version: Optional[int] = None,
        to_version: Optional[int] = None
    ) -> List[BaseEvent]:
        """Get events for a specific aggregate."""
        pass
    
    @abstractmethod
    async def get_events_by_type(
        self, 
        event_type: EventType,
        from_timestamp: Optional[datetime] = None,
        to_timestamp: Optional[datetime] = None
    ) -> List[BaseEvent]:
        """Get events by type within a time range."""
        pass
    
    @abstractmethod
    async def get_events_by_correlation_id(self, correlation_id: UUID) -> List[BaseEvent]:
        """Get events by correlation ID."""
        pass
    
    @abstractmethod
    async def get_all_events(
        self, 
        from_timestamp: Optional[datetime] = None,
        to_timestamp: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> AsyncIterator[BaseEvent]:
        """Get all events with optional filtering."""
        pass


class InMemoryEventStore(IEventStore):
    """In-memory event store implementation for testing and development."""
    
    def __init__(self):
        self._events: List[BaseEvent] = []
        self._lock = asyncio.Lock()
    
    async def append_event(self, event: BaseEvent) -> None:
        """Append a single event to the store."""
        async with self._lock:
            self._events.append(event)
    
    async def append_events(self, events: List[BaseEvent]) -> None:
        """Append multiple events to the store."""
        async with self._lock:
            self._events.extend(events)
    
    async def get_events(
        self, 
        aggregate_id: str, 
        from_version: Optional[int] = None,
        to_version: Optional[int] = None
    ) -> List[BaseEvent]:
        """Get events for a specific aggregate."""
        filtered_events = [
            event for event in self._events 
            if event.aggregate_id == aggregate_id
        ]
        
        if from_version is not None:
            filtered_events = [
                event for event in filtered_events 
                if event.event_version >= from_version
            ]
        
        if to_version is not None:
            filtered_events = [
                event for event in filtered_events 
                if event.event_version <= to_version
            ]
        
        return sorted(filtered_events, key=lambda e: e.event_version)
    
    async def get_events_by_type(
        self, 
        event_type: EventType,
        from_timestamp: Optional[datetime] = None,
        to_timestamp: Optional[datetime] = None
    ) -> List[BaseEvent]:
        """Get events by type within a time range."""
        filtered_events = [
            event for event in self._events 
            if event.event_type == event_type
        ]
        
        if from_timestamp is not None:
            filtered_events = [
                event for event in filtered_events 
                if event.timestamp >= from_timestamp
            ]
        
        if to_timestamp is not None:
            filtered_events = [
                event for event in filtered_events 
                if event.timestamp <= to_timestamp
            ]
        
        return sorted(filtered_events, key=lambda e: e.timestamp)
    
    async def get_events_by_correlation_id(self, correlation_id: UUID) -> List[BaseEvent]:
        """Get events by correlation ID."""
        return [
            event for event in self._events 
            if event.correlation_id == correlation_id
        ]
    
    async def get_all_events(
        self, 
        from_timestamp: Optional[datetime] = None,
        to_timestamp: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> AsyncIterator[BaseEvent]:
        """Get all events with optional filtering."""
        filtered_events = self._events.copy()
        
        if from_timestamp is not None:
            filtered_events = [
                event for event in filtered_events 
                if event.timestamp >= from_timestamp
            ]
        
        if to_timestamp is not None:
            filtered_events = [
                event for event in filtered_events 
                if event.timestamp <= to_timestamp
            ]
        
        sorted_events = sorted(filtered_events, key=lambda e: e.timestamp)
        
        if limit is not None:
            sorted_events = sorted_events[:limit]
        
        for event in sorted_events:
            yield event


class SqlEventStore(IEventStore):
    """SQL-based event store implementation."""
    
    def __init__(self, config: EventStoreConfig):
        self.config = config
        self.engine = create_async_engine(
            config.database_url,
            echo=config.echo_sql,
            pool_size=config.max_connections
        )
        self.async_session = async_sessionmaker(
            self.engine, 
            class_=AsyncSession,
            expire_on_commit=False
        )
    
    async def initialize(self) -> None:
        """Initialize the database tables."""
        if self.config.create_tables:
            async with self.engine.begin() as conn:
                await conn.run_sync(Base.metadata.create_all)
    
    async def close(self) -> None:
        """Close the database connection."""
        await self.engine.dispose()
    
    def _event_to_record(self, event: BaseEvent) -> EventRecord:
        """Convert event to database record."""
        return EventRecord(
            event_id=str(event.event_id),
            event_type=event.event_type.value,
            aggregate_id=event.aggregate_id,
            aggregate_type=event.aggregate_type,
            event_version=event.event_version,
            timestamp=event.timestamp,
            correlation_id=str(event.correlation_id) if event.correlation_id else None,
            causation_id=str(event.causation_id) if event.causation_id else None,
            event_metadata=json.dumps(event.metadata),
            event_data=json.dumps(event.data, default=str)
        )
    
    def _record_to_event(self, record: EventRecord) -> BaseEvent:
        """Convert database record to event."""
        return BaseEvent(
            event_id=UUID(record.event_id),
            event_type=EventType(record.event_type),
            aggregate_id=record.aggregate_id,
            aggregate_type=record.aggregate_type,
            event_version=record.event_version,
            timestamp=record.timestamp,
            correlation_id=UUID(record.correlation_id) if record.correlation_id else None,
            causation_id=UUID(record.causation_id) if record.causation_id else None,
            metadata=json.loads(record.event_metadata),
            data=json.loads(record.event_data)
        )
    
    async def append_event(self, event: BaseEvent) -> None:
        """Append a single event to the store."""
        async with self.async_session() as session:
            record = self._event_to_record(event)
            session.add(record)
            await session.commit()
    
    async def append_events(self, events: List[BaseEvent]) -> None:
        """Append multiple events to the store."""
        async with self.async_session() as session:
            records = [self._event_to_record(event) for event in events]
            session.add_all(records)
            await session.commit()
    
    async def get_events(
        self, 
        aggregate_id: str, 
        from_version: Optional[int] = None,
        to_version: Optional[int] = None
    ) -> List[BaseEvent]:
        """Get events for a specific aggregate."""
        async with self.async_session() as session:
            query = session.query(EventRecord).filter(
                EventRecord.aggregate_id == aggregate_id
            )
            
            if from_version is not None:
                query = query.filter(EventRecord.event_version >= from_version)
            
            if to_version is not None:
                query = query.filter(EventRecord.event_version <= to_version)
            
            query = query.order_by(EventRecord.event_version)
            result = await session.execute(query)
            records = result.scalars().all()
            
            return [self._record_to_event(record) for record in records]
    
    async def get_events_by_type(
        self, 
        event_type: EventType,
        from_timestamp: Optional[datetime] = None,
        to_timestamp: Optional[datetime] = None
    ) -> List[BaseEvent]:
        """Get events by type within a time range."""
        async with self.async_session() as session:
            query = session.query(EventRecord).filter(
                EventRecord.event_type == event_type.value
            )
            
            if from_timestamp is not None:
                query = query.filter(EventRecord.timestamp >= from_timestamp)
            
            if to_timestamp is not None:
                query = query.filter(EventRecord.timestamp <= to_timestamp)
            
            query = query.order_by(EventRecord.timestamp)
            result = await session.execute(query)
            records = result.scalars().all()
            
            return [self._record_to_event(record) for record in records]
    
    async def get_events_by_correlation_id(self, correlation_id: UUID) -> List[BaseEvent]:
        """Get events by correlation ID."""
        async with self.async_session() as session:
            query = session.query(EventRecord).filter(
                EventRecord.correlation_id == str(correlation_id)
            ).order_by(EventRecord.timestamp)
            
            result = await session.execute(query)
            records = result.scalars().all()
            
            return [self._record_to_event(record) for record in records]
    
    async def get_all_events(
        self, 
        from_timestamp: Optional[datetime] = None,
        to_timestamp: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> AsyncIterator[BaseEvent]:
        """Get all events with optional filtering."""
        async with self.async_session() as session:
            query = session.query(EventRecord)
            
            if from_timestamp is not None:
                query = query.filter(EventRecord.timestamp >= from_timestamp)
            
            if to_timestamp is not None:
                query = query.filter(EventRecord.timestamp <= to_timestamp)
            
            query = query.order_by(EventRecord.timestamp)
            
            if limit is not None:
                query = query.limit(limit)
            
            result = await session.execute(query)
            records = result.scalars().all()
            
            for record in records:
                yield self._record_to_event(record)


class EventStore:
    """Main event store class that provides a unified interface."""
    
    def __init__(self, config: Optional[EventStoreConfig] = None, use_memory: bool = False):
        self.config = config or EventStoreConfig()
        
        if use_memory:
            self._store = InMemoryEventStore()
        else:
            self._store = SqlEventStore(self.config)
    
    async def initialize(self) -> None:
        """Initialize the event store."""
        if hasattr(self._store, 'initialize'):
            await self._store.initialize()
    
    async def close(self) -> None:
        """Close the event store."""
        if hasattr(self._store, 'close'):
            await self._store.close()
    
    async def append_event(self, event: BaseEvent) -> None:
        """Append a single event to the store."""
        await self._store.append_event(event)
    
    async def append_events(self, events: List[BaseEvent]) -> None:
        """Append multiple events to the store."""
        await self._store.append_events(events)
    
    async def get_events(
        self, 
        aggregate_id: str, 
        from_version: Optional[int] = None,
        to_version: Optional[int] = None
    ) -> List[BaseEvent]:
        """Get events for a specific aggregate."""
        return await self._store.get_events(aggregate_id, from_version, to_version)
    
    async def get_events_by_type(
        self, 
        event_type: EventType,
        from_timestamp: Optional[datetime] = None,
        to_timestamp: Optional[datetime] = None
    ) -> List[BaseEvent]:
        """Get events by type within a time range."""
        return await self._store.get_events_by_type(event_type, from_timestamp, to_timestamp)
    
    async def get_events_by_correlation_id(self, correlation_id: UUID) -> List[BaseEvent]:
        """Get events by correlation ID."""
        return await self._store.get_events_by_correlation_id(correlation_id)
    
    async def get_all_events(
        self, 
        from_timestamp: Optional[datetime] = None,
        to_timestamp: Optional[datetime] = None,
        limit: Optional[int] = None
    ) -> AsyncIterator[BaseEvent]:
        """Get all events with optional filtering."""
        async for event in self._store.get_all_events(from_timestamp, to_timestamp, limit):
            yield event