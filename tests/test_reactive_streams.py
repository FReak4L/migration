"""
Comprehensive tests for reactive streams implementation.

Tests cover stream processing, backpressure handling, reactive transformation,
stream operators, and flow control.
"""

import pytest
import pytest_asyncio
import asyncio
import time
import random
from typing import AsyncIterator, List, Any
from unittest.mock import Mock, AsyncMock

# Configure pytest-asyncio
pytest_plugins = ('pytest_asyncio',)

from reactive import (
    StreamProcessor, StreamConfig,
    BackpressureHandler, BackpressureConfig, BackpressureStrategy,
    ReactiveTransformer, TransformationConfig,
    StreamOperators,
    FlowController, FlowControlConfig, CongestionAlgorithm
)


class TestStreamProcessor:
    """Test reactive stream processor."""
    
    @pytest_asyncio.fixture
    async def stream_processor(self):
        """Create a stream processor for testing."""
        config = StreamConfig(
            buffer_size=100,
            max_concurrent_operations=5,
            batch_size=10,
            enable_metrics=True
        )
        processor = StreamProcessor(config)
        yield processor
        await processor.stop()
    
    @pytest.mark.asyncio
    async def test_stream_processor_initialization(self, stream_processor):
        """Test stream processor initialization."""
        assert stream_processor.config.buffer_size == 100
        assert stream_processor.config.max_concurrent_operations == 5
        assert stream_processor.state.value == "idle"
    
    async def test_stream_processor_start_stop(self, stream_processor):
        """Test starting and stopping stream processor."""
        await stream_processor.start()
        assert stream_processor.state.value == "running"
        
        await stream_processor.stop()
        assert stream_processor.state.value == "stopped"
    
    async def test_simple_stream_processing(self, stream_processor):
        """Test basic stream processing."""
        async def source():
            for i in range(10):
                yield i
                await asyncio.sleep(0.01)
        
        def processor(item):
            return item * 2
        
        results = []
        async for result in stream_processor.process_stream(source(), processor):
            results.append(result)
        
        assert len(results) == 10
        assert results == [i * 2 for i in range(10)]
    
    async def test_stream_processing_with_errors(self, stream_processor):
        """Test stream processing with error handling."""
        async def source():
            for i in range(5):
                yield i
        
        def processor(item):
            if item == 2:
                raise ValueError("Test error")
            return item * 2
        
        results = []
        errors = []
        
        def error_handler(error):
            errors.append(error)
        
        stream_processor.add_error_handler(error_handler)
        
        try:
            async for result in stream_processor.process_stream(source(), processor):
                results.append(result)
        except ValueError:
            pass  # Expected error
        
        # Should have processed some items before error
        assert len(results) >= 2
        assert len(errors) > 0
    
    async def test_metrics_collection(self, stream_processor):
        """Test metrics collection during processing."""
        async def source():
            for i in range(20):
                yield i
                await asyncio.sleep(0.001)
        
        def processor(item):
            return item
        
        results = []
        async for result in stream_processor.process_stream(source(), processor):
            results.append(result)
        
        metrics = stream_processor.get_metrics_summary()
        assert metrics["items_processed"] == 20
        assert metrics["state"] == "running"
        assert metrics["throughput_per_second"] >= 0


class TestBackpressureHandler:
    """Test backpressure handling strategies."""
    
    @pytest.fixture
    def backpressure_handler(self):
        """Create a backpressure handler for testing."""
        config = BackpressureConfig(
            strategy=BackpressureStrategy.ADAPTIVE,
            buffer_size=50,
            high_watermark=0.8,
            low_watermark=0.3
        )
        return BackpressureHandler(config)
    
    async def test_backpressure_initialization(self, backpressure_handler):
        """Test backpressure handler initialization."""
        assert backpressure_handler.config.strategy == BackpressureStrategy.ADAPTIVE
        assert backpressure_handler.config.buffer_size == 50
    
    async def test_drop_strategy(self):
        """Test drop backpressure strategy."""
        config = BackpressureConfig(
            strategy=BackpressureStrategy.DROP,
            buffer_size=5
        )
        handler = BackpressureHandler(config)
        
        def processor(item):
            return item * 2
        
        # Fill buffer beyond capacity
        results = []
        for i in range(10):
            result = await handler.handle_item(i, processor, i)
            if result is not None:
                results.append(result)
        
        # Should have dropped some items
        assert len(results) <= 5
        assert handler.get_metrics()["dropped_items"] > 0
    
    async def test_block_strategy(self):
        """Test block backpressure strategy."""
        config = BackpressureConfig(
            strategy=BackpressureStrategy.BLOCK,
            buffer_size=3
        )
        handler = BackpressureHandler(config)
        
        def processor(item):
            return item * 2
        
        start_time = time.time()
        results = []
        
        # Process items that should cause blocking
        for i in range(5):
            result = await handler.handle_item(i, processor, i)
            if result is not None:
                results.append(result)
        
        processing_time = time.time() - start_time
        
        # Should have processed all items but with some blocking
        assert len(results) == 5
        assert handler.get_metrics()["blocked_time_seconds"] > 0
    
    async def test_sample_strategy(self):
        """Test sample backpressure strategy."""
        config = BackpressureConfig(
            strategy=BackpressureStrategy.SAMPLE,
            sample_rate=0.5  # 50% sampling
        )
        handler = BackpressureHandler(config)
        
        def processor(item):
            return item * 2
        
        results = []
        for i in range(20):
            result = await handler.handle_item(i, processor, 10)  # High buffer usage
            if result is not None:
                results.append(result)
            await asyncio.sleep(0.01)  # Small delay for sampling
        
        # Should have sampled approximately 50% of items
        assert len(results) < 20
        assert len(results) > 5  # But not too few
    
    async def test_circuit_breaker_strategy(self):
        """Test circuit breaker backpressure strategy."""
        config = BackpressureConfig(
            strategy=BackpressureStrategy.CIRCUIT_BREAKER,
            circuit_breaker_threshold=3
        )
        handler = BackpressureHandler(config)
        
        def failing_processor(item):
            raise ValueError("Simulated failure")
        
        results = []
        errors = 0
        
        # Cause multiple failures to trip circuit breaker
        for i in range(10):
            try:
                result = await handler.handle_item(i, failing_processor, 5)
                if result is not None:
                    results.append(result)
            except ValueError:
                errors += 1
        
        metrics = handler.get_metrics()
        assert metrics["circuit_open"] == True
        assert metrics["consecutive_failures"] >= 3


class TestReactiveTransformer:
    """Test reactive data transformer."""
    
    @pytest_asyncio.fixture
    async def reactive_transformer(self):
        """Create a reactive transformer for testing."""
        stream_config = StreamConfig(buffer_size=50, batch_size=5)
        backpressure_config = BackpressureConfig(strategy=BackpressureStrategy.ADAPTIVE)
        transformation_config = TransformationConfig(
            max_workers=2,
            chunk_size=10,
            enable_caching=True
        )
        
        transformer = ReactiveTransformer(
            stream_config, backpressure_config, transformation_config
        )
        yield transformer
        await transformer.shutdown()
    
    async def test_transformer_initialization(self, reactive_transformer):
        """Test transformer initialization."""
        assert reactive_transformer.transformation_config.max_workers == 2
        assert reactive_transformer.transformation_config.enable_caching == True
    
    async def test_individual_transformation(self, reactive_transformer):
        """Test individual item transformation."""
        async def source():
            for i in range(10):
                yield i
        
        def transformer(item):
            return item ** 2
        
        results = []
        async for result in reactive_transformer.transform_stream(source(), transformer):
            results.append(result)
        
        assert len(results) == 10
        assert results == [i ** 2 for i in range(10)]
    
    async def test_batch_transformation(self, reactive_transformer):
        """Test batch transformation."""
        async def source():
            for i in range(15):
                yield i
        
        def transformer(item):
            return item * 3
        
        def batch_processor(items):
            # Add 100 to each item in batch
            return [item + 100 for item in items]
        
        results = []
        async for batch in reactive_transformer.transform_stream(
            source(), transformer, batch_processor=batch_processor
        ):
            results.extend(batch)
        
        assert len(results) == 15
        # Each item should be transformed (i * 3) then batch processed (+ 100)
        assert results[0] == 100  # (0 * 3) + 100
        assert results[1] == 103  # (1 * 3) + 100
    
    async def test_transformation_with_validation(self, reactive_transformer):
        """Test transformation with validation."""
        async def source():
            for i in range(10):
                yield i
        
        def transformer(item):
            return item * 2
        
        def validator(item):
            return item < 10  # Only accept items less than 10
        
        results = []
        async for result in reactive_transformer.transform_stream(
            source(), transformer, validator
        ):
            results.append(result)
        
        # Should only get items where (i * 2) < 10, so i < 5
        assert len(results) == 5
        assert all(item < 10 for item in results)
    
    async def test_transformation_caching(self, reactive_transformer):
        """Test transformation result caching."""
        call_count = 0
        
        def expensive_transformer(item):
            nonlocal call_count
            call_count += 1
            time.sleep(0.01)  # Simulate expensive operation
            return item * 10
        
        # Transform same item multiple times
        result1 = await reactive_transformer.transform_single(5, expensive_transformer)
        result2 = await reactive_transformer.transform_single(5, expensive_transformer)
        result3 = await reactive_transformer.transform_single(5, expensive_transformer)
        
        assert result1 == result2 == result3 == 50
        
        # Should have cached the result after first call
        metrics = reactive_transformer.get_metrics()
        assert metrics["cache_hits"] >= 2
        assert call_count == 1  # Only called once due to caching
    
    async def test_async_transformation(self, reactive_transformer):
        """Test async transformation functions."""
        async def source():
            for i in range(5):
                yield i
        
        async def async_transformer(item):
            await asyncio.sleep(0.01)  # Simulate async work
            return item * 5
        
        results = []
        async for result in reactive_transformer.transform_stream(source(), async_transformer):
            results.append(result)
        
        assert len(results) == 5
        assert results == [i * 5 for i in range(5)]


class TestStreamOperators:
    """Test stream operators."""
    
    async def test_map_operator(self):
        """Test map stream operator."""
        async def source():
            for i in range(5):
                yield i
        
        def mapper(x):
            return x * 2
        
        results = []
        async for result in StreamOperators.map(source(), mapper):
            results.append(result)
        
        assert results == [0, 2, 4, 6, 8]
    
    async def test_filter_operator(self):
        """Test filter stream operator."""
        async def source():
            for i in range(10):
                yield i
        
        def predicate(x):
            return x % 2 == 0  # Even numbers only
        
        results = []
        async for result in StreamOperators.filter(source(), predicate):
            results.append(result)
        
        assert results == [0, 2, 4, 6, 8]
    
    async def test_take_operator(self):
        """Test take stream operator."""
        async def source():
            for i in range(100):
                yield i
        
        results = []
        async for result in StreamOperators.take(source(), 5):
            results.append(result)
        
        assert results == [0, 1, 2, 3, 4]
    
    async def test_skip_operator(self):
        """Test skip stream operator."""
        async def source():
            for i in range(10):
                yield i
        
        results = []
        async for result in StreamOperators.skip(source(), 3):
            results.append(result)
        
        assert results == [3, 4, 5, 6, 7, 8, 9]
    
    async def test_batch_operator(self):
        """Test batch stream operator."""
        async def source():
            for i in range(10):
                yield i
        
        batches = []
        async for batch in StreamOperators.batch(source(), 3):
            batches.append(batch)
        
        assert len(batches) == 4  # [0,1,2], [3,4,5], [6,7,8], [9]
        assert batches[0] == [0, 1, 2]
        assert batches[1] == [3, 4, 5]
        assert batches[2] == [6, 7, 8]
        assert batches[3] == [9]
    
    async def test_distinct_operator(self):
        """Test distinct stream operator."""
        async def source():
            for item in [1, 2, 2, 3, 1, 4, 3, 5]:
                yield item
        
        results = []
        async for result in StreamOperators.distinct(source()):
            results.append(result)
        
        assert results == [1, 2, 3, 4, 5]
    
    async def test_scan_operator(self):
        """Test scan stream operator."""
        async def source():
            for i in range(1, 6):  # 1, 2, 3, 4, 5
                yield i
        
        def accumulator(acc, x):
            return acc + x
        
        results = []
        async for result in StreamOperators.scan(source(), accumulator, 0):
            results.append(result)
        
        assert results == [0, 1, 3, 6, 10, 15]  # Running sum
    
    async def test_reduce_operator(self):
        """Test reduce stream operator."""
        async def source():
            for i in range(1, 6):  # 1, 2, 3, 4, 5
                yield i
        
        def reducer(acc, x):
            return acc + x
        
        result = await StreamOperators.reduce(source(), reducer, 0)
        assert result == 15  # Sum of 1+2+3+4+5
    
    async def test_throttle_operator(self):
        """Test throttle stream operator."""
        async def source():
            for i in range(5):
                yield i
                await asyncio.sleep(0.01)
        
        start_time = time.time()
        results = []
        
        async for result in StreamOperators.throttle(source(), 0.05):  # 50ms throttle
            results.append(result)
        
        elapsed = time.time() - start_time
        
        assert len(results) == 5
        assert elapsed >= 0.2  # Should take at least 4 * 50ms = 200ms


class TestFlowController:
    """Test flow control mechanisms."""
    
    @pytest.fixture
    def flow_controller(self):
        """Create a flow controller for testing."""
        config = FlowControlConfig(
            initial_rate=100.0,
            max_rate=1000.0,
            min_rate=10.0,
            congestion_algorithm=CongestionAlgorithm.ADAPTIVE
        )
        return FlowController(config)
    
    async def test_flow_controller_initialization(self, flow_controller):
        """Test flow controller initialization."""
        assert flow_controller.current_rate == 100.0
        assert flow_controller.config.max_rate == 1000.0
        assert flow_controller.state.value == "normal"
    
    async def test_normal_operation(self, flow_controller):
        """Test flow control during normal operation."""
        # Simulate good performance metrics
        new_rate = await flow_controller.control_flow(
            queue_length=10,
            processing_latency=0.01,
            throughput=95.0
        )
        
        # Rate should increase during normal operation
        assert new_rate >= 100.0
        assert flow_controller.state.value == "normal"
    
    async def test_congestion_detection(self, flow_controller):
        """Test congestion detection and response."""
        # Simulate congestion conditions
        for _ in range(5):
            await flow_controller.control_flow(
                queue_length=500,  # High queue length
                processing_latency=0.5,  # High latency
                throughput=20.0  # Low throughput
            )
        
        metrics = flow_controller.get_metrics()
        
        # Should detect congestion and reduce rate
        assert metrics["congestion_events"] > 0
        assert flow_controller.current_rate < 100.0
    
    async def test_aimd_algorithm(self):
        """Test AIMD congestion control algorithm."""
        config = FlowControlConfig(
            initial_rate=100.0,
            congestion_algorithm=CongestionAlgorithm.AIMD,
            recovery_factor=0.5
        )
        controller = FlowController(config)
        
        # Trigger congestion
        await controller.control_flow(
            queue_length=1000,
            processing_latency=1.0,
            throughput=10.0
        )
        
        # Rate should be reduced multiplicatively
        assert controller.current_rate < 100.0
        
        # Normal operation should increase additively
        for _ in range(5):
            await controller.control_flow(
                queue_length=5,
                processing_latency=0.01,
                throughput=controller.current_rate * 0.9
            )
        
        # Rate should gradually increase
        assert controller.current_rate > controller.config.initial_rate * 0.5
    
    async def test_bbr_algorithm(self):
        """Test BBR congestion control algorithm."""
        config = FlowControlConfig(
            initial_rate=100.0,
            congestion_algorithm=CongestionAlgorithm.BBR
        )
        controller = FlowController(config)
        
        # Provide consistent metrics to establish bandwidth estimate
        for _ in range(10):
            await controller.control_flow(
                queue_length=10,
                processing_latency=0.02,
                throughput=150.0
            )
        
        # BBR should adjust rate based on bandwidth estimation
        metrics = controller.get_metrics()
        assert metrics["rate_adjustments"] > 0
    
    async def test_adaptive_algorithm(self):
        """Test adaptive congestion control algorithm."""
        config = FlowControlConfig(
            initial_rate=100.0,
            congestion_algorithm=CongestionAlgorithm.ADAPTIVE,
            enable_prediction=True
        )
        controller = FlowController(config)
        
        # Provide varying performance data for learning
        throughputs = [80, 90, 95, 85, 75, 70, 60, 50]
        
        for i, throughput in enumerate(throughputs):
            await controller.control_flow(
                queue_length=i * 10,
                processing_latency=0.01 * (i + 1),
                throughput=throughput
            )
        
        metrics = controller.get_metrics()
        
        # Adaptive algorithm should make adjustments based on learning
        assert metrics["rate_adjustments"] > 0
        assert len(controller.adaptive_history) > 0
    
    async def test_flow_controller_health_check(self, flow_controller):
        """Test flow controller health monitoring."""
        # Initially should be healthy
        assert flow_controller.is_healthy() == True
        
        # Simulate severe congestion
        for _ in range(10):
            await flow_controller.control_flow(
                queue_length=10000,  # Extreme queue length
                processing_latency=5.0,  # Very high latency
                throughput=1.0  # Very low throughput
            )
        
        # May become unhealthy under extreme conditions
        # (depending on the specific algorithm and thresholds)
        metrics = flow_controller.get_metrics()
        assert metrics["congestion_events"] > 0


class TestIntegration:
    """Integration tests combining multiple reactive components."""
    
    async def test_end_to_end_reactive_pipeline(self):
        """Test complete reactive processing pipeline."""
        # Create components
        stream_config = StreamConfig(buffer_size=100, batch_size=10)
        backpressure_config = BackpressureConfig(strategy=BackpressureStrategy.ADAPTIVE)
        transformation_config = TransformationConfig(enable_caching=True)
        flow_config = FlowControlConfig(initial_rate=50.0)
        
        transformer = ReactiveTransformer(
            stream_config, backpressure_config, transformation_config
        )
        flow_controller = FlowController(flow_config)
        
        try:
            # Create data source
            async def data_source():
                for i in range(100):
                    yield {"id": i, "value": i * 10}
                    await asyncio.sleep(0.001)
            
            # Define transformation pipeline
            def transform_item(item):
                return {
                    "id": item["id"],
                    "value": item["value"],
                    "processed": True,
                    "timestamp": time.time()
                }
            
            def validate_item(item):
                return item["value"] >= 0 and "processed" in item
            
            # Process through reactive pipeline
            results = []
            processing_times = []
            
            async for result in transformer.transform_stream(
                data_source(), transform_item, validate_item
            ):
                start_time = time.time()
                
                # Simulate flow control
                current_rate = await flow_controller.control_flow(
                    queue_length=len(results),
                    processing_latency=0.01,
                    throughput=len(results) / max(time.time() - start_time + 0.001, 0.001)
                )
                
                results.append(result)
                processing_times.append(time.time() - start_time)
                
                # Apply rate limiting
                if current_rate > 0:
                    await asyncio.sleep(1.0 / current_rate)
            
            # Verify results
            assert len(results) == 100
            assert all(item["processed"] for item in results)
            assert all(item["value"] >= 0 for item in results)
            
            # Check metrics
            transformer_metrics = transformer.get_metrics()
            flow_metrics = flow_controller.get_metrics()
            
            assert transformer_metrics["transformations_completed"] == 100
            assert transformer_metrics["validation_failures"] == 0
            assert flow_metrics["rate_adjustments"] >= 0
            
        finally:
            await transformer.shutdown()
    
    async def test_reactive_stream_operators_chain(self):
        """Test chaining multiple stream operators."""
        async def source():
            for i in range(50):
                yield i
        
        # Chain multiple operators
        mapped = StreamOperators.map(source(), lambda x: x * 2)
        filtered = StreamOperators.filter(mapped, lambda x: x % 4 == 0)
        batched = StreamOperators.batch(filtered, 5)
        taken = StreamOperators.take(batched, 3)
        
        results = []
        async for batch in taken:
            results.extend(batch)
        
        # Should have processed: 0*2=0, 2*2=4, 4*2=8, 6*2=12, 8*2=16, etc.
        # Filtered to multiples of 4: 0, 4, 8, 12, 16, 20, 24, 28, 32, 36, 40, 44, 48
        # Batched in groups of 5, taken first 3 batches = 15 items
        assert len(results) == 15
        assert all(x % 4 == 0 for x in results)
        assert results[0] == 0
        assert results[1] == 4
        assert results[2] == 8