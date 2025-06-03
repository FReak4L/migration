"""
Event Bus Implementation with Apache Kafka

This module provides event bus functionality for publishing and subscribing to events
using Apache Kafka as the message broker.
"""

import json
import asyncio
import logging
from typing import List, Dict, Any, Callable, Optional, Set
from uuid import UUID
from datetime import datetime
from abc import ABC, abstractmethod
from pydantic import BaseModel
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer
from aiokafka.errors import KafkaError

from .event_models import BaseEvent, EventType


logger = logging.getLogger(__name__)


class EventBusConfig(BaseModel):
    """Configuration for event bus."""
    
    kafka_bootstrap_servers: str = "localhost:9092"
    topic_prefix: str = "migration"
    producer_config: Dict[str, Any] = {}
    consumer_config: Dict[str, Any] = {}
    retry_attempts: int = 3
    retry_delay: float = 1.0
    enable_dead_letter_queue: bool = True
    dead_letter_topic: str = "migration-dead-letter"


class IEventBus(ABC):
    """Interface for event bus implementations."""
    
    @abstractmethod
    async def publish(self, event: BaseEvent) -> None:
        """Publish a single event."""
        pass
    
    @abstractmethod
    async def publish_batch(self, events: List[BaseEvent]) -> None:
        """Publish multiple events."""
        pass
    
    @abstractmethod
    async def subscribe(
        self, 
        event_types: List[EventType], 
        handler: Callable[[BaseEvent], None],
        consumer_group: str = "default"
    ) -> None:
        """Subscribe to specific event types."""
        pass
    
    @abstractmethod
    async def start(self) -> None:
        """Start the event bus."""
        pass
    
    @abstractmethod
    async def stop(self) -> None:
        """Stop the event bus."""
        pass


class InMemoryEventBus(IEventBus):
    """In-memory event bus implementation for testing."""
    
    def __init__(self):
        self._subscribers: Dict[EventType, List[Callable[[BaseEvent], None]]] = {}
        self._running = False
    
    async def publish(self, event: BaseEvent) -> None:
        """Publish a single event."""
        if not self._running:
            return
        
        handlers = self._subscribers.get(event.event_type, [])
        for handler in handlers:
            try:
                if asyncio.iscoroutinefunction(handler):
                    await handler(event)
                else:
                    handler(event)
            except Exception as e:
                logger.error(f"Error in event handler: {e}")
    
    async def publish_batch(self, events: List[BaseEvent]) -> None:
        """Publish multiple events."""
        for event in events:
            await self.publish(event)
    
    async def subscribe(
        self, 
        event_types: List[EventType], 
        handler: Callable[[BaseEvent], None],
        consumer_group: str = "default"
    ) -> None:
        """Subscribe to specific event types."""
        for event_type in event_types:
            if event_type not in self._subscribers:
                self._subscribers[event_type] = []
            self._subscribers[event_type].append(handler)
    
    async def start(self) -> None:
        """Start the event bus."""
        self._running = True
    
    async def stop(self) -> None:
        """Stop the event bus."""
        self._running = False


class KafkaEventBus(IEventBus):
    """Kafka-based event bus implementation."""
    
    def __init__(self, config: EventBusConfig):
        self.config = config
        self._producer: Optional[AIOKafkaProducer] = None
        self._consumers: Dict[str, AIOKafkaConsumer] = {}
        self._consumer_tasks: Set[asyncio.Task] = set()
        self._running = False
    
    def _get_topic_name(self, event_type: EventType) -> str:
        """Get Kafka topic name for event type."""
        return f"{self.config.topic_prefix}.{event_type.value.replace('.', '_')}"
    
    def _serialize_event(self, event: BaseEvent) -> bytes:
        """Serialize event to bytes."""
        event_dict = event.dict()
        # Convert UUID and datetime objects to strings
        event_dict['event_id'] = str(event_dict['event_id'])
        if event_dict.get('correlation_id'):
            event_dict['correlation_id'] = str(event_dict['correlation_id'])
        if event_dict.get('causation_id'):
            event_dict['causation_id'] = str(event_dict['causation_id'])
        event_dict['timestamp'] = event_dict['timestamp'].isoformat()
        
        return json.dumps(event_dict).encode('utf-8')
    
    def _deserialize_event(self, data: bytes) -> BaseEvent:
        """Deserialize bytes to event."""
        event_dict = json.loads(data.decode('utf-8'))
        
        # Convert string UUIDs back to UUID objects
        event_dict['event_id'] = UUID(event_dict['event_id'])
        if event_dict.get('correlation_id'):
            event_dict['correlation_id'] = UUID(event_dict['correlation_id'])
        if event_dict.get('causation_id'):
            event_dict['causation_id'] = UUID(event_dict['causation_id'])
        
        # Convert ISO timestamp back to datetime
        event_dict['timestamp'] = datetime.fromisoformat(event_dict['timestamp'])
        
        return BaseEvent(**event_dict)
    
    async def _create_producer(self) -> AIOKafkaProducer:
        """Create Kafka producer."""
        producer_config = {
            'bootstrap_servers': self.config.kafka_bootstrap_servers,
            'value_serializer': lambda v: v,  # We handle serialization manually
            **self.config.producer_config
        }
        
        producer = AIOKafkaProducer(**producer_config)
        await producer.start()
        return producer
    
    async def _create_consumer(
        self, 
        topics: List[str], 
        consumer_group: str
    ) -> AIOKafkaConsumer:
        """Create Kafka consumer."""
        consumer_config = {
            'bootstrap_servers': self.config.kafka_bootstrap_servers,
            'group_id': consumer_group,
            'value_deserializer': lambda v: v,  # We handle deserialization manually
            **self.config.consumer_config
        }
        
        consumer = AIOKafkaConsumer(*topics, **consumer_config)
        await consumer.start()
        return consumer
    
    async def _handle_consumer_messages(
        self, 
        consumer: AIOKafkaConsumer,
        handler: Callable[[BaseEvent], None]
    ) -> None:
        """Handle messages from Kafka consumer."""
        try:
            async for message in consumer:
                try:
                    event = self._deserialize_event(message.value)
                    
                    # Execute handler with retry logic
                    for attempt in range(self.config.retry_attempts):
                        try:
                            if asyncio.iscoroutinefunction(handler):
                                await handler(event)
                            else:
                                handler(event)
                            break
                        except Exception as e:
                            logger.warning(
                                f"Handler failed (attempt {attempt + 1}/{self.config.retry_attempts}): {e}"
                            )
                            if attempt < self.config.retry_attempts - 1:
                                await asyncio.sleep(self.config.retry_delay * (attempt + 1))
                            else:
                                # Send to dead letter queue if enabled
                                if self.config.enable_dead_letter_queue:
                                    await self._send_to_dead_letter_queue(event, str(e))
                                logger.error(f"Handler failed after all retries: {e}")
                
                except Exception as e:
                    logger.error(f"Error processing message: {e}")
        
        except Exception as e:
            logger.error(f"Consumer error: {e}")
    
    async def _send_to_dead_letter_queue(self, event: BaseEvent, error: str) -> None:
        """Send failed event to dead letter queue."""
        try:
            if self._producer:
                dead_letter_event = event.copy()
                dead_letter_event.metadata['error'] = error
                dead_letter_event.metadata['failed_at'] = datetime.utcnow().isoformat()
                
                serialized_event = self._serialize_event(dead_letter_event)
                await self._producer.send(
                    self.config.dead_letter_topic,
                    value=serialized_event,
                    key=str(event.event_id).encode('utf-8')
                )
        except Exception as e:
            logger.error(f"Failed to send event to dead letter queue: {e}")
    
    async def publish(self, event: BaseEvent) -> None:
        """Publish a single event."""
        if not self._producer or not self._running:
            raise RuntimeError("Event bus not started")
        
        topic = self._get_topic_name(event.event_type)
        serialized_event = self._serialize_event(event)
        
        try:
            await self._producer.send(
                topic,
                value=serialized_event,
                key=str(event.event_id).encode('utf-8')
            )
        except KafkaError as e:
            logger.error(f"Failed to publish event: {e}")
            raise
    
    async def publish_batch(self, events: List[BaseEvent]) -> None:
        """Publish multiple events."""
        if not self._producer or not self._running:
            raise RuntimeError("Event bus not started")
        
        try:
            # Group events by topic for efficient batching
            events_by_topic: Dict[str, List[BaseEvent]] = {}
            for event in events:
                topic = self._get_topic_name(event.event_type)
                if topic not in events_by_topic:
                    events_by_topic[topic] = []
                events_by_topic[topic].append(event)
            
            # Send events in batches
            for topic, topic_events in events_by_topic.items():
                for event in topic_events:
                    serialized_event = self._serialize_event(event)
                    await self._producer.send(
                        topic,
                        value=serialized_event,
                        key=str(event.event_id).encode('utf-8')
                    )
            
            # Ensure all messages are sent
            await self._producer.flush()
        
        except KafkaError as e:
            logger.error(f"Failed to publish batch events: {e}")
            raise
    
    async def subscribe(
        self, 
        event_types: List[EventType], 
        handler: Callable[[BaseEvent], None],
        consumer_group: str = "default"
    ) -> None:
        """Subscribe to specific event types."""
        if not self._running:
            raise RuntimeError("Event bus not started")
        
        topics = [self._get_topic_name(event_type) for event_type in event_types]
        consumer_key = f"{consumer_group}:{':'.join(topics)}"
        
        if consumer_key not in self._consumers:
            consumer = await self._create_consumer(topics, consumer_group)
            self._consumers[consumer_key] = consumer
            
            # Start consumer task
            task = asyncio.create_task(
                self._handle_consumer_messages(consumer, handler)
            )
            self._consumer_tasks.add(task)
            task.add_done_callback(self._consumer_tasks.discard)
    
    async def start(self) -> None:
        """Start the event bus."""
        if self._running:
            return
        
        self._producer = await self._create_producer()
        self._running = True
        logger.info("Kafka event bus started")
    
    async def stop(self) -> None:
        """Stop the event bus."""
        if not self._running:
            return
        
        self._running = False
        
        # Cancel all consumer tasks
        for task in self._consumer_tasks:
            task.cancel()
        
        if self._consumer_tasks:
            await asyncio.gather(*self._consumer_tasks, return_exceptions=True)
        
        # Stop all consumers
        for consumer in self._consumers.values():
            await consumer.stop()
        self._consumers.clear()
        
        # Stop producer
        if self._producer:
            await self._producer.stop()
            self._producer = None
        
        logger.info("Kafka event bus stopped")


class EventBus:
    """Main event bus class that provides a unified interface."""
    
    def __init__(self, config: Optional[EventBusConfig] = None, use_memory: bool = False):
        self.config = config or EventBusConfig()
        
        if use_memory:
            self._bus = InMemoryEventBus()
        else:
            self._bus = KafkaEventBus(self.config)
    
    async def start(self) -> None:
        """Start the event bus."""
        await self._bus.start()
    
    async def stop(self) -> None:
        """Stop the event bus."""
        await self._bus.stop()
    
    async def publish(self, event: BaseEvent) -> None:
        """Publish a single event."""
        await self._bus.publish(event)
    
    async def publish_batch(self, events: List[BaseEvent]) -> None:
        """Publish multiple events."""
        await self._bus.publish_batch(events)
    
    async def subscribe(
        self, 
        event_types: List[EventType], 
        handler: Callable[[BaseEvent], None],
        consumer_group: str = "default"
    ) -> None:
        """Subscribe to specific event types."""
        await self._bus.subscribe(event_types, handler, consumer_group)