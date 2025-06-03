"""
Reactive Stream Processor with Backpressure Support

This module implements a high-performance reactive stream processor for handling
large-scale data migration with automatic backpressure management.
"""

import asyncio
import logging
from typing import AsyncIterator, Callable, Optional, Any, Dict, List
from dataclasses import dataclass, field
from enum import Enum
import time
from collections import deque
import weakref

logger = logging.getLogger(__name__)


class StreamState(Enum):
    """Stream processing states."""
    IDLE = "idle"
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"
    ERROR = "error"


@dataclass
class StreamConfig:
    """Configuration for reactive stream processing."""
    buffer_size: int = 1000
    max_concurrent_operations: int = 10
    backpressure_threshold: float = 0.8  # Trigger backpressure at 80% buffer capacity
    batch_size: int = 100
    processing_timeout: float = 30.0
    enable_metrics: bool = True
    enable_flow_control: bool = True
    max_retry_attempts: int = 3
    retry_delay: float = 1.0


@dataclass
class StreamMetrics:
    """Metrics for stream processing performance."""
    items_processed: int = 0
    items_failed: int = 0
    items_retried: int = 0
    backpressure_events: int = 0
    average_processing_time: float = 0.0
    peak_buffer_usage: int = 0
    throughput_per_second: float = 0.0
    start_time: float = field(default_factory=time.time)
    last_update: float = field(default_factory=time.time)


class StreamProcessor:
    """
    High-performance reactive stream processor with backpressure support.
    
    Features:
    - Automatic backpressure handling
    - Configurable buffer management
    - Concurrent processing with flow control
    - Comprehensive metrics and monitoring
    - Error handling and retry logic
    """
    
    def __init__(self, config: StreamConfig = None):
        self.config = config or StreamConfig()
        self.state = StreamState.IDLE
        self.metrics = StreamMetrics()
        
        # Internal buffers and queues
        self._input_buffer = deque(maxlen=self.config.buffer_size)
        self._output_buffer = deque(maxlen=self.config.buffer_size)
        self._processing_queue = asyncio.Queue(maxsize=self.config.max_concurrent_operations)
        
        # Flow control
        self._backpressure_active = False
        self._processing_semaphore = asyncio.Semaphore(self.config.max_concurrent_operations)
        
        # Event handlers
        self._subscribers = weakref.WeakSet()
        self._error_handlers = []
        
        # Processing tasks
        self._processor_tasks = []
        self._monitor_task = None
        
        logger.info(f"StreamProcessor initialized with config: {self.config}")
    
    async def start(self) -> None:
        """Start the stream processor."""
        if self.state != StreamState.IDLE:
            raise RuntimeError(f"Cannot start processor in state: {self.state}")
        
        self.state = StreamState.RUNNING
        self.metrics.start_time = time.time()
        
        # Start processor tasks
        for i in range(self.config.max_concurrent_operations):
            task = asyncio.create_task(self._process_items())
            self._processor_tasks.append(task)
        
        # Start monitoring task
        if self.config.enable_metrics:
            self._monitor_task = asyncio.create_task(self._monitor_performance())
        
        logger.info("StreamProcessor started")
    
    async def stop(self) -> None:
        """Stop the stream processor gracefully."""
        if self.state == StreamState.STOPPED:
            return
        
        self.state = StreamState.STOPPED
        
        # Cancel all tasks
        for task in self._processor_tasks:
            task.cancel()
        
        if self._monitor_task:
            self._monitor_task.cancel()
        
        # Wait for tasks to complete
        await asyncio.gather(*self._processor_tasks, return_exceptions=True)
        if self._monitor_task:
            await asyncio.gather(self._monitor_task, return_exceptions=True)
        
        logger.info("StreamProcessor stopped")
    
    async def process_stream(
        self,
        source: AsyncIterator[Any],
        processor: Callable[[Any], Any],
        sink: Optional[Callable[[Any], None]] = None
    ) -> AsyncIterator[Any]:
        """
        Process a stream of data with backpressure handling.
        
        Args:
            source: Async iterator providing input data
            processor: Function to process each item
            sink: Optional function to handle processed items
            
        Yields:
            Processed items
        """
        if self.state != StreamState.RUNNING:
            await self.start()
        
        try:
            # Start consuming from source
            consumer_task = asyncio.create_task(
                self._consume_source(source)
            )
            
            # Start processing items
            async for processed_item in self._process_with_backpressure(processor):
                if sink:
                    await self._safe_call(sink, processed_item)
                yield processed_item
            
            # Wait for consumer to complete
            await consumer_task
            
        except Exception as e:
            self.state = StreamState.ERROR
            await self._handle_error(e)
            raise
    
    async def _consume_source(self, source: AsyncIterator[Any]) -> None:
        """Consume items from source and add to input buffer."""
        try:
            async for item in source:
                # Check for backpressure
                while self._should_apply_backpressure():
                    await asyncio.sleep(0.1)  # Wait for buffer to drain
                
                self._input_buffer.append(item)
                
                # Update metrics
                if len(self._input_buffer) > self.metrics.peak_buffer_usage:
                    self.metrics.peak_buffer_usage = len(self._input_buffer)
                
        except Exception as e:
            logger.error(f"Error consuming source: {e}")
            await self._handle_error(e)
    
    async def _process_with_backpressure(
        self, 
        processor: Callable[[Any], Any]
    ) -> AsyncIterator[Any]:
        """Process items with backpressure management."""
        while self.state == StreamState.RUNNING or self._input_buffer:
            # Wait for items to process
            if not self._input_buffer:
                await asyncio.sleep(0.01)
                continue
            
            # Get batch of items
            batch = []
            batch_size = min(self.config.batch_size, len(self._input_buffer))
            
            for _ in range(batch_size):
                if self._input_buffer:
                    batch.append(self._input_buffer.popleft())
            
            if not batch:
                continue
            
            # Process batch concurrently
            async with self._processing_semaphore:
                tasks = [
                    self._process_single_item(item, processor)
                    for item in batch
                ]
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for result in results:
                    if isinstance(result, Exception):
                        await self._handle_processing_error(result)
                    else:
                        yield result
                        self.metrics.items_processed += 1
    
    async def _process_single_item(
        self, 
        item: Any, 
        processor: Callable[[Any], Any]
    ) -> Any:
        """Process a single item with error handling and retries."""
        start_time = time.time()
        
        for attempt in range(self.config.max_retry_attempts + 1):
            try:
                # Apply timeout
                result = await asyncio.wait_for(
                    self._safe_call(processor, item),
                    timeout=self.config.processing_timeout
                )
                
                # Update processing time metrics
                processing_time = time.time() - start_time
                self._update_processing_time(processing_time)
                
                return result
                
            except asyncio.TimeoutError:
                logger.warning(f"Processing timeout for item: {item}")
                if attempt < self.config.max_retry_attempts:
                    await asyncio.sleep(self.config.retry_delay * (attempt + 1))
                    self.metrics.items_retried += 1
                else:
                    raise
                    
            except Exception as e:
                logger.error(f"Processing error for item {item}: {e}")
                if attempt < self.config.max_retry_attempts:
                    await asyncio.sleep(self.config.retry_delay * (attempt + 1))
                    self.metrics.items_retried += 1
                else:
                    self.metrics.items_failed += 1
                    raise
    
    async def _process_items(self) -> None:
        """Background task for processing items from the queue."""
        while self.state == StreamState.RUNNING:
            try:
                # Wait for items in processing queue
                await asyncio.sleep(0.01)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in processor task: {e}")
                await self._handle_error(e)
    
    def _should_apply_backpressure(self) -> bool:
        """Check if backpressure should be applied."""
        buffer_usage = len(self._input_buffer) / self.config.buffer_size
        
        if buffer_usage >= self.config.backpressure_threshold:
            if not self._backpressure_active:
                self._backpressure_active = True
                self.metrics.backpressure_events += 1
                logger.warning(f"Backpressure activated - buffer usage: {buffer_usage:.2%}")
            return True
        else:
            if self._backpressure_active:
                self._backpressure_active = False
                logger.info("Backpressure deactivated")
            return False
    
    async def _monitor_performance(self) -> None:
        """Monitor and update performance metrics."""
        while self.state == StreamState.RUNNING:
            try:
                await asyncio.sleep(1.0)  # Update metrics every second
                
                current_time = time.time()
                elapsed = current_time - self.metrics.last_update
                
                if elapsed > 0:
                    # Calculate throughput
                    items_since_last = self.metrics.items_processed
                    self.metrics.throughput_per_second = items_since_last / elapsed
                    self.metrics.last_update = current_time
                
                # Log metrics periodically
                if int(current_time) % 10 == 0:  # Every 10 seconds
                    logger.info(f"Stream metrics: {self.get_metrics_summary()}")
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in performance monitor: {e}")
    
    def _update_processing_time(self, processing_time: float) -> None:
        """Update average processing time metrics."""
        if self.metrics.items_processed == 0:
            self.metrics.average_processing_time = processing_time
        else:
            # Exponential moving average
            alpha = 0.1
            self.metrics.average_processing_time = (
                alpha * processing_time + 
                (1 - alpha) * self.metrics.average_processing_time
            )
    
    async def _safe_call(self, func: Callable, *args, **kwargs) -> Any:
        """Safely call a function, handling both sync and async."""
        try:
            if asyncio.iscoroutinefunction(func):
                return await func(*args, **kwargs)
            else:
                return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"Error calling function {func.__name__}: {e}")
            raise
    
    async def _handle_error(self, error: Exception) -> None:
        """Handle processing errors."""
        logger.error(f"Stream processing error: {error}")
        
        # Notify error handlers
        for handler in self._error_handlers:
            try:
                await self._safe_call(handler, error)
            except Exception as e:
                logger.error(f"Error in error handler: {e}")
    
    async def _handle_processing_error(self, error: Exception) -> None:
        """Handle individual item processing errors."""
        self.metrics.items_failed += 1
        await self._handle_error(error)
    
    def add_error_handler(self, handler: Callable[[Exception], None]) -> None:
        """Add an error handler."""
        self._error_handlers.append(handler)
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get a summary of current metrics."""
        runtime = time.time() - self.metrics.start_time
        
        return {
            "state": self.state.value,
            "runtime_seconds": round(runtime, 2),
            "items_processed": self.metrics.items_processed,
            "items_failed": self.metrics.items_failed,
            "items_retried": self.metrics.items_retried,
            "backpressure_events": self.metrics.backpressure_events,
            "average_processing_time_ms": round(self.metrics.average_processing_time * 1000, 2),
            "peak_buffer_usage": self.metrics.peak_buffer_usage,
            "throughput_per_second": round(self.metrics.throughput_per_second, 2),
            "buffer_usage_percent": round(len(self._input_buffer) / self.config.buffer_size * 100, 1),
            "backpressure_active": self._backpressure_active
        }
    
    def get_detailed_metrics(self) -> StreamMetrics:
        """Get detailed metrics object."""
        return self.metrics
    
    async def pause(self) -> None:
        """Pause stream processing."""
        if self.state == StreamState.RUNNING:
            self.state = StreamState.PAUSED
            logger.info("Stream processing paused")
    
    async def resume(self) -> None:
        """Resume stream processing."""
        if self.state == StreamState.PAUSED:
            self.state = StreamState.RUNNING
            logger.info("Stream processing resumed")
    
    def is_healthy(self) -> bool:
        """Check if the stream processor is healthy."""
        return (
            self.state in [StreamState.RUNNING, StreamState.PAUSED] and
            self.metrics.items_failed / max(self.metrics.items_processed, 1) < 0.1  # Less than 10% failure rate
        )