"""
Reactive Data Transformer with Stream Processing

This module provides reactive transformation capabilities for migration data
with built-in backpressure handling and stream processing optimizations.
"""

import asyncio
import logging
from typing import AsyncIterator, Callable, Optional, Any, Dict, List, Union
from dataclasses import dataclass
import time
from concurrent.futures import ThreadPoolExecutor
import functools

from .stream_processor import StreamProcessor, StreamConfig
from .backpressure_handler import BackpressureHandler, BackpressureConfig, BackpressureStrategy

logger = logging.getLogger(__name__)


@dataclass
class TransformationConfig:
    """Configuration for reactive transformation."""
    max_workers: int = 4
    chunk_size: int = 100
    enable_parallel_processing: bool = True
    transformation_timeout: float = 30.0
    enable_caching: bool = True
    cache_size: int = 1000
    enable_validation: bool = True


class ReactiveTransformer:
    """
    High-performance reactive data transformer with stream processing.
    
    Features:
    - Reactive stream processing with backpressure
    - Parallel transformation execution
    - Built-in caching and validation
    - Comprehensive error handling
    - Performance monitoring and metrics
    """
    
    def __init__(
        self,
        stream_config: StreamConfig = None,
        backpressure_config: BackpressureConfig = None,
        transformation_config: TransformationConfig = None
    ):
        self.stream_config = stream_config or StreamConfig()
        self.backpressure_config = backpressure_config or BackpressureConfig()
        self.transformation_config = transformation_config or TransformationConfig()
        
        # Initialize components
        self.stream_processor = StreamProcessor(self.stream_config)
        self.backpressure_handler = BackpressureHandler(self.backpressure_config)
        
        # Thread pool for CPU-intensive transformations
        self.executor = ThreadPoolExecutor(
            max_workers=self.transformation_config.max_workers
        )
        
        # Caching
        self._cache = {} if self.transformation_config.enable_caching else None
        self._cache_hits = 0
        self._cache_misses = 0
        
        # Metrics
        self._transformation_count = 0
        self._validation_failures = 0
        self._start_time = time.time()
        
        logger.info("ReactiveTransformer initialized")
    
    async def transform_stream(
        self,
        source: AsyncIterator[Any],
        transformer: Callable[[Any], Any],
        validator: Optional[Callable[[Any], bool]] = None,
        batch_processor: Optional[Callable[[List[Any]], List[Any]]] = None
    ) -> AsyncIterator[Any]:
        """
        Transform a stream of data reactively with backpressure handling.
        
        Args:
            source: Source stream of data
            transformer: Function to transform each item
            validator: Optional validation function
            batch_processor: Optional batch processing function
            
        Yields:
            Transformed and validated items
        """
        await self.stream_processor.start()
        
        try:
            if batch_processor:
                async for batch in self._process_in_batches(source, transformer, validator, batch_processor):
                    for item in batch:
                        yield item
            else:
                async for item in self._process_individually(source, transformer, validator):
                    yield item
                    
        finally:
            await self.stream_processor.stop()
    
    async def _process_individually(
        self,
        source: AsyncIterator[Any],
        transformer: Callable[[Any], Any],
        validator: Optional[Callable[[Any], bool]]
    ) -> AsyncIterator[Any]:
        """Process items individually through the reactive stream."""
        
        async def process_item(item: Any) -> Any:
            # Check cache first
            if self._cache is not None:
                cache_key = self._get_cache_key(item)
                if cache_key in self._cache:
                    self._cache_hits += 1
                    return self._cache[cache_key]
                self._cache_misses += 1
            
            # Transform the item
            transformed = await self._apply_transformation(item, transformer)
            
            # Validate if validator provided
            if validator and self.transformation_config.enable_validation:
                if not await self._apply_validation(transformed, validator):
                    self._validation_failures += 1
                    raise ValueError(f"Validation failed for transformed item: {transformed}")
            
            # Cache the result
            if self._cache is not None:
                self._cache[cache_key] = transformed
                # Limit cache size
                if len(self._cache) > self.transformation_config.cache_size:
                    # Remove oldest entry (simple FIFO)
                    oldest_key = next(iter(self._cache))
                    del self._cache[oldest_key]
            
            self._transformation_count += 1
            return transformed
        
        # Process through reactive stream
        async for result in self.stream_processor.process_stream(
            source, 
            process_item
        ):
            yield result
    
    async def _process_in_batches(
        self,
        source: AsyncIterator[Any],
        transformer: Callable[[Any], Any],
        validator: Optional[Callable[[Any], bool]],
        batch_processor: Callable[[List[Any]], List[Any]]
    ) -> AsyncIterator[List[Any]]:
        """Process items in batches for better performance."""
        
        batch = []
        
        async for item in source:
            batch.append(item)
            
            if len(batch) >= self.transformation_config.chunk_size:
                processed_batch = await self._process_batch(
                    batch, transformer, validator, batch_processor
                )
                yield processed_batch
                batch = []
        
        # Process remaining items
        if batch:
            processed_batch = await self._process_batch(
                batch, transformer, validator, batch_processor
            )
            yield processed_batch
    
    async def _process_batch(
        self,
        batch: List[Any],
        transformer: Callable[[Any], Any],
        validator: Optional[Callable[[Any], bool]],
        batch_processor: Callable[[List[Any]], List[Any]]
    ) -> List[Any]:
        """Process a batch of items."""
        
        # Transform individual items first
        transformed_items = []
        for item in batch:
            try:
                transformed = await self._apply_transformation(item, transformer)
                transformed_items.append(transformed)
            except Exception as e:
                logger.error(f"Error transforming item in batch: {e}")
                # Continue with other items
        
        # Apply batch processor
        if batch_processor:
            try:
                batch_result = await self._apply_batch_processing(
                    transformed_items, batch_processor
                )
                transformed_items = batch_result
            except Exception as e:
                logger.error(f"Error in batch processing: {e}")
        
        # Validate batch results
        if validator and self.transformation_config.enable_validation:
            validated_items = []
            for item in transformed_items:
                try:
                    if await self._apply_validation(item, validator):
                        validated_items.append(item)
                    else:
                        self._validation_failures += 1
                        logger.warning(f"Validation failed for item: {item}")
                except Exception as e:
                    logger.error(f"Error validating item: {e}")
                    self._validation_failures += 1
            
            transformed_items = validated_items
        
        self._transformation_count += len(transformed_items)
        return transformed_items
    
    async def _apply_transformation(
        self, 
        item: Any, 
        transformer: Callable[[Any], Any]
    ) -> Any:
        """Apply transformation to an item."""
        try:
            if self.transformation_config.enable_parallel_processing:
                # Run in thread pool for CPU-intensive operations
                if not asyncio.iscoroutinefunction(transformer):
                    loop = asyncio.get_event_loop()
                    result = await loop.run_in_executor(
                        self.executor, 
                        transformer, 
                        item
                    )
                else:
                    result = await asyncio.wait_for(
                        transformer(item),
                        timeout=self.transformation_config.transformation_timeout
                    )
            else:
                # Run synchronously
                if asyncio.iscoroutinefunction(transformer):
                    result = await transformer(item)
                else:
                    result = transformer(item)
            
            return result
            
        except asyncio.TimeoutError:
            logger.error(f"Transformation timeout for item: {item}")
            raise
        except Exception as e:
            logger.error(f"Transformation error for item {item}: {e}")
            raise
    
    async def _apply_validation(
        self, 
        item: Any, 
        validator: Callable[[Any], bool]
    ) -> bool:
        """Apply validation to an item."""
        try:
            if asyncio.iscoroutinefunction(validator):
                return await validator(item)
            else:
                return validator(item)
        except Exception as e:
            logger.error(f"Validation error for item {item}: {e}")
            return False
    
    async def _apply_batch_processing(
        self, 
        items: List[Any], 
        batch_processor: Callable[[List[Any]], List[Any]]
    ) -> List[Any]:
        """Apply batch processing to a list of items."""
        try:
            if asyncio.iscoroutinefunction(batch_processor):
                return await batch_processor(items)
            else:
                # Run in thread pool for CPU-intensive batch operations
                loop = asyncio.get_event_loop()
                return await loop.run_in_executor(
                    self.executor,
                    batch_processor,
                    items
                )
        except Exception as e:
            logger.error(f"Batch processing error: {e}")
            raise
    
    def _get_cache_key(self, item: Any) -> str:
        """Generate a cache key for an item."""
        try:
            # Simple hash-based key
            return str(hash(str(item)))
        except Exception:
            # Fallback to string representation
            return str(item)[:100]  # Limit key length
    
    async def transform_single(
        self, 
        item: Any, 
        transformer: Callable[[Any], Any],
        validator: Optional[Callable[[Any], bool]] = None
    ) -> Any:
        """Transform a single item (convenience method)."""
        transformed = await self._apply_transformation(item, transformer)
        
        if validator and self.transformation_config.enable_validation:
            if not await self._apply_validation(transformed, validator):
                raise ValueError(f"Validation failed for transformed item: {transformed}")
        
        return transformed
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get transformation metrics."""
        runtime = time.time() - self._start_time
        
        cache_hit_rate = (
            self._cache_hits / (self._cache_hits + self._cache_misses)
            if (self._cache_hits + self._cache_misses) > 0 else 0.0
        )
        
        return {
            "transformations_completed": self._transformation_count,
            "validation_failures": self._validation_failures,
            "cache_hits": self._cache_hits,
            "cache_misses": self._cache_misses,
            "cache_hit_rate": round(cache_hit_rate, 3),
            "runtime_seconds": round(runtime, 2),
            "transformations_per_second": round(self._transformation_count / max(runtime, 1), 2),
            "stream_processor_metrics": self.stream_processor.get_metrics_summary(),
            "backpressure_metrics": self.backpressure_handler.get_metrics()
        }
    
    def clear_cache(self) -> None:
        """Clear the transformation cache."""
        if self._cache is not None:
            self._cache.clear()
            self._cache_hits = 0
            self._cache_misses = 0
            logger.info("Transformation cache cleared")
    
    async def shutdown(self) -> None:
        """Shutdown the reactive transformer."""
        await self.stream_processor.stop()
        self.executor.shutdown(wait=True)
        logger.info("ReactiveTransformer shutdown complete")
    
    def is_healthy(self) -> bool:
        """Check if the transformer is healthy."""
        failure_rate = (
            self._validation_failures / max(self._transformation_count, 1)
        )
        
        return (
            self.stream_processor.is_healthy() and
            self.backpressure_handler.is_healthy() and
            failure_rate < 0.1  # Less than 10% validation failures
        )