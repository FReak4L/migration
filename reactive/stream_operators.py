"""
Stream Operators for Reactive Programming

This module provides a comprehensive set of stream operators for building
complex reactive data processing pipelines.
"""

import asyncio
import logging
from typing import AsyncIterator, Callable, Optional, Any, Dict, List, Union, TypeVar, Generic
from dataclasses import dataclass
import time
from collections import defaultdict, deque
import heapq

logger = logging.getLogger(__name__)

T = TypeVar('T')
U = TypeVar('U')


class StreamOperators:
    """
    Collection of reactive stream operators for data transformation and processing.
    
    Provides functional programming style operators for building complex
    data processing pipelines with reactive streams.
    """
    
    @staticmethod
    async def map(
        source: AsyncIterator[T], 
        mapper: Callable[[T], U]
    ) -> AsyncIterator[U]:
        """
        Transform each item in the stream using the mapper function.
        
        Args:
            source: Source stream
            mapper: Function to transform each item
            
        Yields:
            Transformed items
        """
        async for item in source:
            try:
                if asyncio.iscoroutinefunction(mapper):
                    result = await mapper(item)
                else:
                    result = mapper(item)
                yield result
            except Exception as e:
                logger.error(f"Error in map operation: {e}")
                # Continue processing other items
    
    @staticmethod
    async def filter(
        source: AsyncIterator[T], 
        predicate: Callable[[T], bool]
    ) -> AsyncIterator[T]:
        """
        Filter items in the stream based on predicate.
        
        Args:
            source: Source stream
            predicate: Function to test each item
            
        Yields:
            Items that pass the predicate
        """
        async for item in source:
            try:
                if asyncio.iscoroutinefunction(predicate):
                    should_include = await predicate(item)
                else:
                    should_include = predicate(item)
                
                if should_include:
                    yield item
            except Exception as e:
                logger.error(f"Error in filter operation: {e}")
    
    @staticmethod
    async def flat_map(
        source: AsyncIterator[T], 
        mapper: Callable[[T], AsyncIterator[U]]
    ) -> AsyncIterator[U]:
        """
        Transform each item to a stream and flatten the results.
        
        Args:
            source: Source stream
            mapper: Function that returns a stream for each item
            
        Yields:
            Flattened items from all sub-streams
        """
        async for item in source:
            try:
                sub_stream = mapper(item)
                async for sub_item in sub_stream:
                    yield sub_item
            except Exception as e:
                logger.error(f"Error in flat_map operation: {e}")
    
    @staticmethod
    async def take(
        source: AsyncIterator[T], 
        count: int
    ) -> AsyncIterator[T]:
        """
        Take only the first 'count' items from the stream.
        
        Args:
            source: Source stream
            count: Number of items to take
            
        Yields:
            First 'count' items
        """
        taken = 0
        async for item in source:
            if taken >= count:
                break
            yield item
            taken += 1
    
    @staticmethod
    async def skip(
        source: AsyncIterator[T], 
        count: int
    ) -> AsyncIterator[T]:
        """
        Skip the first 'count' items from the stream.
        
        Args:
            source: Source stream
            count: Number of items to skip
            
        Yields:
            Items after skipping 'count' items
        """
        skipped = 0
        async for item in source:
            if skipped < count:
                skipped += 1
                continue
            yield item
    
    @staticmethod
    async def batch(
        source: AsyncIterator[T], 
        size: int
    ) -> AsyncIterator[List[T]]:
        """
        Group items into batches of specified size.
        
        Args:
            source: Source stream
            size: Batch size
            
        Yields:
            Batches of items
        """
        batch = []
        async for item in source:
            batch.append(item)
            if len(batch) >= size:
                yield batch
                batch = []
        
        # Yield remaining items
        if batch:
            yield batch
    
    @staticmethod
    async def window(
        source: AsyncIterator[T], 
        size: int, 
        step: int = 1
    ) -> AsyncIterator[List[T]]:
        """
        Create sliding windows of items.
        
        Args:
            source: Source stream
            size: Window size
            step: Step size between windows
            
        Yields:
            Sliding windows of items
        """
        window_items = deque(maxlen=size)
        items_processed = 0
        
        async for item in source:
            window_items.append(item)
            items_processed += 1
            
            if len(window_items) == size and (items_processed - size) % step == 0:
                yield list(window_items)
    
    @staticmethod
    async def distinct(
        source: AsyncIterator[T], 
        key_func: Optional[Callable[[T], Any]] = None
    ) -> AsyncIterator[T]:
        """
        Remove duplicate items from the stream.
        
        Args:
            source: Source stream
            key_func: Optional function to extract comparison key
            
        Yields:
            Unique items
        """
        seen = set()
        async for item in source:
            try:
                key = key_func(item) if key_func else item
                if key not in seen:
                    seen.add(key)
                    yield item
            except Exception as e:
                logger.error(f"Error in distinct operation: {e}")
    
    @staticmethod
    async def distinct_until_changed(
        source: AsyncIterator[T], 
        key_func: Optional[Callable[[T], Any]] = None
    ) -> AsyncIterator[T]:
        """
        Remove consecutive duplicate items.
        
        Args:
            source: Source stream
            key_func: Optional function to extract comparison key
            
        Yields:
            Items without consecutive duplicates
        """
        last_key = object()  # Sentinel value
        
        async for item in source:
            try:
                key = key_func(item) if key_func else item
                if key != last_key:
                    yield item
                    last_key = key
            except Exception as e:
                logger.error(f"Error in distinct_until_changed operation: {e}")
    
    @staticmethod
    async def throttle(
        source: AsyncIterator[T], 
        interval: float
    ) -> AsyncIterator[T]:
        """
        Throttle the stream to emit at most one item per interval.
        
        Args:
            source: Source stream
            interval: Minimum time between emissions (seconds)
            
        Yields:
            Throttled items
        """
        last_emission = 0.0
        
        async for item in source:
            current_time = time.time()
            if current_time - last_emission >= interval:
                yield item
                last_emission = current_time
            else:
                # Wait for the remaining time
                wait_time = interval - (current_time - last_emission)
                await asyncio.sleep(wait_time)
                yield item
                last_emission = time.time()
    
    @staticmethod
    async def debounce(
        source: AsyncIterator[T], 
        delay: float
    ) -> AsyncIterator[T]:
        """
        Debounce the stream - emit item only after delay with no new items.
        
        Args:
            source: Source stream
            delay: Debounce delay (seconds)
            
        Yields:
            Debounced items
        """
        last_item = None
        last_time = 0.0
        
        async for item in source:
            last_item = item
            last_time = time.time()
            
            # Wait for the delay period
            await asyncio.sleep(delay)
            
            # Check if this is still the latest item
            if time.time() - last_time >= delay and item == last_item:
                yield item
    
    @staticmethod
    async def scan(
        source: AsyncIterator[T], 
        accumulator: Callable[[U, T], U], 
        initial: U
    ) -> AsyncIterator[U]:
        """
        Apply accumulator function and emit intermediate results.
        
        Args:
            source: Source stream
            accumulator: Function to accumulate values
            initial: Initial accumulator value
            
        Yields:
            Accumulated values
        """
        acc = initial
        yield acc
        
        async for item in source:
            try:
                if asyncio.iscoroutinefunction(accumulator):
                    acc = await accumulator(acc, item)
                else:
                    acc = accumulator(acc, item)
                yield acc
            except Exception as e:
                logger.error(f"Error in scan operation: {e}")
    
    @staticmethod
    async def reduce(
        source: AsyncIterator[T], 
        reducer: Callable[[U, T], U], 
        initial: U
    ) -> U:
        """
        Reduce the stream to a single value.
        
        Args:
            source: Source stream
            reducer: Function to reduce values
            initial: Initial value
            
        Returns:
            Final reduced value
        """
        acc = initial
        async for item in source:
            try:
                if asyncio.iscoroutinefunction(reducer):
                    acc = await reducer(acc, item)
                else:
                    acc = reducer(acc, item)
            except Exception as e:
                logger.error(f"Error in reduce operation: {e}")
        return acc
    
    @staticmethod
    async def group_by(
        source: AsyncIterator[T], 
        key_func: Callable[[T], Any]
    ) -> AsyncIterator[tuple[Any, List[T]]]:
        """
        Group items by key function.
        
        Args:
            source: Source stream
            key_func: Function to extract grouping key
            
        Yields:
            Tuples of (key, list_of_items)
        """
        groups = defaultdict(list)
        
        async for item in source:
            try:
                key = key_func(item)
                groups[key].append(item)
            except Exception as e:
                logger.error(f"Error in group_by operation: {e}")
        
        for key, items in groups.items():
            yield (key, items)
    
    @staticmethod
    async def merge(*sources: AsyncIterator[T]) -> AsyncIterator[T]:
        """
        Merge multiple streams into one.
        
        Args:
            sources: Multiple source streams
            
        Yields:
            Items from all sources
        """
        async def consume_source(source: AsyncIterator[T], queue: asyncio.Queue):
            try:
                async for item in source:
                    await queue.put(item)
            except Exception as e:
                logger.error(f"Error consuming source in merge: {e}")
            finally:
                await queue.put(None)  # Signal completion
        
        queue = asyncio.Queue()
        tasks = []
        
        # Start consuming all sources
        for source in sources:
            task = asyncio.create_task(consume_source(source, queue))
            tasks.append(task)
        
        completed_sources = 0
        total_sources = len(sources)
        
        while completed_sources < total_sources:
            item = await queue.get()
            if item is None:
                completed_sources += 1
            else:
                yield item
        
        # Wait for all tasks to complete
        await asyncio.gather(*tasks, return_exceptions=True)
    
    @staticmethod
    async def zip(*sources: AsyncIterator[T]) -> AsyncIterator[tuple]:
        """
        Zip multiple streams together.
        
        Args:
            sources: Multiple source streams
            
        Yields:
            Tuples of items from all sources
        """
        iterators = [aiter(source) for source in sources]
        
        while True:
            try:
                items = []
                for iterator in iterators:
                    item = await anext(iterator)
                    items.append(item)
                yield tuple(items)
            except StopAsyncIteration:
                break
    
    @staticmethod
    async def buffer_time(
        source: AsyncIterator[T], 
        time_span: float
    ) -> AsyncIterator[List[T]]:
        """
        Buffer items for a specified time span.
        
        Args:
            source: Source stream
            time_span: Time to buffer items (seconds)
            
        Yields:
            Buffers of items collected over time span
        """
        buffer = []
        last_emission = time.time()
        
        async for item in source:
            buffer.append(item)
            current_time = time.time()
            
            if current_time - last_emission >= time_span:
                if buffer:
                    yield buffer
                    buffer = []
                    last_emission = current_time
        
        # Emit remaining buffer
        if buffer:
            yield buffer
    
    @staticmethod
    async def retry(
        source: AsyncIterator[T], 
        max_attempts: int = 3, 
        delay: float = 1.0
    ) -> AsyncIterator[T]:
        """
        Retry failed operations in the stream.
        
        Args:
            source: Source stream
            max_attempts: Maximum retry attempts
            delay: Delay between retries
            
        Yields:
            Items with retry logic applied
        """
        async for item in source:
            for attempt in range(max_attempts):
                try:
                    yield item
                    break
                except Exception as e:
                    if attempt == max_attempts - 1:
                        logger.error(f"Failed after {max_attempts} attempts: {e}")
                        raise
                    else:
                        logger.warning(f"Attempt {attempt + 1} failed, retrying: {e}")
                        await asyncio.sleep(delay * (attempt + 1))
    
    @staticmethod
    async def timeout(
        source: AsyncIterator[T], 
        timeout_seconds: float
    ) -> AsyncIterator[T]:
        """
        Apply timeout to stream operations.
        
        Args:
            source: Source stream
            timeout_seconds: Timeout in seconds
            
        Yields:
            Items within timeout
        """
        async for item in source:
            try:
                yield await asyncio.wait_for(
                    asyncio.coroutine(lambda: item)(),
                    timeout=timeout_seconds
                )
            except asyncio.TimeoutError:
                logger.error(f"Timeout after {timeout_seconds} seconds")
                raise