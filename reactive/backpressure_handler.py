"""
Advanced Backpressure Handling for Reactive Streams

This module implements sophisticated backpressure strategies to handle
high-volume data streams without overwhelming downstream systems.
"""

import asyncio
import logging
from typing import Any, Callable, Optional, Dict, List
from dataclasses import dataclass
from enum import Enum
import time
import math

logger = logging.getLogger(__name__)


class BackpressureStrategy(Enum):
    """Different backpressure handling strategies."""
    DROP = "drop"  # Drop oldest items when buffer is full
    BLOCK = "block"  # Block upstream until buffer has space
    SAMPLE = "sample"  # Sample items at regular intervals
    ADAPTIVE = "adaptive"  # Dynamically adjust based on processing speed
    CIRCUIT_BREAKER = "circuit_breaker"  # Stop processing when overwhelmed


@dataclass
class BackpressureConfig:
    """Configuration for backpressure handling."""
    strategy: BackpressureStrategy = BackpressureStrategy.ADAPTIVE
    buffer_size: int = 1000
    high_watermark: float = 0.8  # Trigger backpressure at 80%
    low_watermark: float = 0.3   # Resume normal processing at 30%
    sample_rate: float = 0.1     # For sampling strategy (10%)
    adaptive_window: int = 100   # Window size for adaptive calculations
    circuit_breaker_threshold: int = 5  # Consecutive failures to trip circuit
    recovery_timeout: float = 30.0  # Time to wait before retry


class BackpressureHandler:
    """
    Advanced backpressure handler with multiple strategies.
    
    Provides intelligent backpressure management to prevent system overload
    while maintaining optimal throughput.
    """
    
    def __init__(self, config: BackpressureConfig = None):
        self.config = config or BackpressureConfig()
        self._buffer = []
        self._dropped_items = 0
        self._blocked_time = 0.0
        self._last_sample_time = 0.0
        self._processing_times = []
        self._consecutive_failures = 0
        self._circuit_open = False
        self._last_circuit_trip = 0.0
        
        # Adaptive strategy state
        self._throughput_history = []
        self._optimal_rate = None
        
        logger.info(f"BackpressureHandler initialized with strategy: {self.config.strategy}")
    
    async def handle_item(
        self, 
        item: Any, 
        processor: Callable[[Any], Any],
        current_buffer_size: int
    ) -> Optional[Any]:
        """
        Handle an item according to the configured backpressure strategy.
        
        Args:
            item: The item to process
            processor: Function to process the item
            current_buffer_size: Current size of the processing buffer
            
        Returns:
            Processed item or None if dropped/blocked
        """
        buffer_usage = current_buffer_size / self.config.buffer_size
        
        # Check circuit breaker
        if self._should_trip_circuit():
            return await self._handle_circuit_breaker(item, processor)
        
        # Apply backpressure strategy
        if buffer_usage >= self.config.high_watermark:
            return await self._apply_backpressure(item, processor, buffer_usage)
        else:
            return await self._process_normally(item, processor)
    
    async def _apply_backpressure(
        self, 
        item: Any, 
        processor: Callable[[Any], Any],
        buffer_usage: float
    ) -> Optional[Any]:
        """Apply the configured backpressure strategy."""
        strategy = self.config.strategy
        
        if strategy == BackpressureStrategy.DROP:
            return await self._handle_drop_strategy(item, processor)
        elif strategy == BackpressureStrategy.BLOCK:
            return await self._handle_block_strategy(item, processor)
        elif strategy == BackpressureStrategy.SAMPLE:
            return await self._handle_sample_strategy(item, processor)
        elif strategy == BackpressureStrategy.ADAPTIVE:
            return await self._handle_adaptive_strategy(item, processor, buffer_usage)
        elif strategy == BackpressureStrategy.CIRCUIT_BREAKER:
            return await self._handle_circuit_breaker(item, processor)
        else:
            # Default to blocking
            return await self._handle_block_strategy(item, processor)
    
    async def _handle_drop_strategy(
        self, 
        item: Any, 
        processor: Callable[[Any], Any]
    ) -> Optional[Any]:
        """Drop strategy: Drop items when buffer is full."""
        if len(self._buffer) >= self.config.buffer_size:
            # Drop oldest item
            if self._buffer:
                dropped = self._buffer.pop(0)
                self._dropped_items += 1
                logger.debug(f"Dropped item due to backpressure: {dropped}")
        
        # Add new item to buffer
        self._buffer.append(item)
        return await self._process_from_buffer(processor)
    
    async def _handle_block_strategy(
        self, 
        item: Any, 
        processor: Callable[[Any], Any]
    ) -> Optional[Any]:
        """Block strategy: Wait until buffer has space."""
        start_time = time.time()
        
        # Wait for buffer space
        while len(self._buffer) >= self.config.buffer_size:
            await asyncio.sleep(0.01)  # Small delay to prevent busy waiting
            
            # Process items from buffer to make space
            if self._buffer:
                await self._process_from_buffer(processor)
        
        self._blocked_time += time.time() - start_time
        
        # Add item to buffer
        self._buffer.append(item)
        return await self._process_from_buffer(processor)
    
    async def _handle_sample_strategy(
        self, 
        item: Any, 
        processor: Callable[[Any], Any]
    ) -> Optional[Any]:
        """Sample strategy: Process items at regular intervals."""
        current_time = time.time()
        
        # Check if enough time has passed for next sample
        if current_time - self._last_sample_time >= (1.0 / self.config.sample_rate):
            self._last_sample_time = current_time
            return await self._process_normally(item, processor)
        else:
            # Skip this item
            logger.debug(f"Skipped item due to sampling: {item}")
            return None
    
    async def _handle_adaptive_strategy(
        self, 
        item: Any, 
        processor: Callable[[Any], Any],
        buffer_usage: float
    ) -> Optional[Any]:
        """Adaptive strategy: Adjust processing rate based on performance."""
        # Calculate optimal processing rate
        if not self._optimal_rate:
            self._optimal_rate = await self._calculate_optimal_rate()
        
        # Adjust processing based on buffer usage
        if buffer_usage > 0.9:  # Critical level
            # Process multiple items to catch up
            batch_size = min(3, len(self._buffer))
            results = []
            for _ in range(batch_size):
                if self._buffer:
                    buffered_item = self._buffer.pop(0)
                    result = await self._process_normally(buffered_item, processor)
                    if result:
                        results.append(result)
            
            # Process current item
            current_result = await self._process_normally(item, processor)
            if current_result:
                results.append(current_result)
            
            return results[-1] if results else None
        else:
            # Normal processing with rate limiting
            return await self._process_with_rate_limit(item, processor)
    
    async def _handle_circuit_breaker(
        self, 
        item: Any, 
        processor: Callable[[Any], Any]
    ) -> Optional[Any]:
        """Circuit breaker strategy: Stop processing when overwhelmed."""
        if self._circuit_open:
            # Check if recovery timeout has passed
            if time.time() - self._last_circuit_trip > self.config.recovery_timeout:
                logger.info("Circuit breaker recovery attempt")
                self._circuit_open = False
                self._consecutive_failures = 0
            else:
                logger.debug("Circuit breaker open - dropping item")
                return None
        
        try:
            result = await self._process_normally(item, processor)
            self._consecutive_failures = 0  # Reset on success
            return result
        except Exception as e:
            self._consecutive_failures += 1
            if self._consecutive_failures >= self.config.circuit_breaker_threshold:
                self._circuit_open = True
                self._last_circuit_trip = time.time()
                logger.warning(f"Circuit breaker tripped after {self._consecutive_failures} failures")
            raise
    
    async def _process_normally(
        self, 
        item: Any, 
        processor: Callable[[Any], Any]
    ) -> Any:
        """Process an item normally without backpressure."""
        start_time = time.time()
        
        try:
            if asyncio.iscoroutinefunction(processor):
                result = await processor(item)
            else:
                result = processor(item)
            
            # Record processing time
            processing_time = time.time() - start_time
            self._processing_times.append(processing_time)
            
            # Keep only recent processing times
            if len(self._processing_times) > self.config.adaptive_window:
                self._processing_times.pop(0)
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing item: {e}")
            raise
    
    async def _process_from_buffer(self, processor: Callable[[Any], Any]) -> Optional[Any]:
        """Process an item from the internal buffer."""
        if not self._buffer:
            return None
        
        item = self._buffer.pop(0)
        return await self._process_normally(item, processor)
    
    async def _process_with_rate_limit(
        self, 
        item: Any, 
        processor: Callable[[Any], Any]
    ) -> Any:
        """Process item with rate limiting based on optimal rate."""
        if self._optimal_rate:
            # Calculate delay based on optimal rate
            delay = 1.0 / self._optimal_rate
            await asyncio.sleep(delay)
        
        return await self._process_normally(item, processor)
    
    async def _calculate_optimal_rate(self) -> float:
        """Calculate optimal processing rate based on historical data."""
        if not self._processing_times:
            return 10.0  # Default rate
        
        # Calculate average processing time
        avg_processing_time = sum(self._processing_times) / len(self._processing_times)
        
        # Optimal rate is inverse of processing time with safety margin
        optimal_rate = 1.0 / (avg_processing_time * 1.2)  # 20% safety margin
        
        logger.debug(f"Calculated optimal rate: {optimal_rate:.2f} items/second")
        return optimal_rate
    
    def _should_trip_circuit(self) -> bool:
        """Check if circuit breaker should be tripped."""
        return (
            self.config.strategy == BackpressureStrategy.CIRCUIT_BREAKER and
            self._consecutive_failures >= self.config.circuit_breaker_threshold
        )
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get backpressure handling metrics."""
        avg_processing_time = (
            sum(self._processing_times) / len(self._processing_times)
            if self._processing_times else 0.0
        )
        
        return {
            "strategy": self.config.strategy.value,
            "buffer_size": len(self._buffer),
            "dropped_items": self._dropped_items,
            "blocked_time_seconds": round(self._blocked_time, 2),
            "average_processing_time_ms": round(avg_processing_time * 1000, 2),
            "consecutive_failures": self._consecutive_failures,
            "circuit_open": self._circuit_open,
            "optimal_rate": self._optimal_rate
        }
    
    def reset_metrics(self) -> None:
        """Reset all metrics."""
        self._dropped_items = 0
        self._blocked_time = 0.0
        self._processing_times.clear()
        self._consecutive_failures = 0
        self._circuit_open = False
        self._throughput_history.clear()
        logger.info("Backpressure metrics reset")
    
    async def drain_buffer(self, processor: Callable[[Any], Any]) -> List[Any]:
        """Drain all items from the buffer."""
        results = []
        while self._buffer:
            result = await self._process_from_buffer(processor)
            if result is not None:
                results.append(result)
        return results
    
    def is_healthy(self) -> bool:
        """Check if backpressure handler is healthy."""
        return (
            not self._circuit_open and
            self._consecutive_failures < self.config.circuit_breaker_threshold and
            len(self._buffer) < self.config.buffer_size * 0.9
        )