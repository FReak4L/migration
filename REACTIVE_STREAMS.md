# Reactive Streams Implementation

## Overview

This document describes the comprehensive reactive streams implementation for the migration tool, providing high-performance data processing with automatic backpressure handling, flow control, and advanced stream operators.

## Architecture

### Core Components

1. **StreamProcessor**: High-performance reactive stream processor with backpressure support
2. **BackpressureHandler**: Advanced backpressure handling with multiple strategies
3. **ReactiveTransformer**: Reactive data transformer with stream processing optimizations
4. **StreamOperators**: Comprehensive set of functional stream operators
5. **FlowController**: Sophisticated flow control with multiple congestion algorithms

## Features

### 🔄 Reactive Stream Processing
- **Asynchronous Processing**: Full async/await support for high concurrency
- **Automatic Backpressure**: Intelligent backpressure handling to prevent overload
- **Configurable Buffering**: Adjustable buffer sizes and processing parameters
- **Error Handling**: Comprehensive error handling with retry logic
- **Metrics Collection**: Real-time performance monitoring and metrics

### 🛡️ Backpressure Strategies
- **Drop Strategy**: Drop oldest items when buffer is full
- **Block Strategy**: Block upstream until buffer has space
- **Sample Strategy**: Sample items at regular intervals
- **Adaptive Strategy**: Dynamically adjust based on processing speed
- **Circuit Breaker**: Stop processing when overwhelmed

### 🏗️ Flow Control Algorithms
- **AIMD**: Additive Increase Multiplicative Decrease
- **CUBIC**: CUBIC congestion control algorithm
- **BBR**: Bottleneck Bandwidth and Round-trip propagation time
- **Adaptive**: Machine learning-based adaptive control

### 🔧 Stream Operators
- **Transformation**: map, flat_map, scan, reduce
- **Filtering**: filter, distinct, distinct_until_changed
- **Windowing**: batch, window, buffer_time
- **Flow Control**: throttle, debounce, timeout
- **Utility**: take, skip, merge, zip

## Quick Start

### Basic Stream Processing

```python
from reactive import StreamProcessor, StreamConfig

# Configure stream processor
config = StreamConfig(
    buffer_size=1000,
    max_concurrent_operations=10,
    batch_size=100,
    enable_metrics=True
)

processor = StreamProcessor(config)

# Define data source
async def data_source():
    for i in range(1000):
        yield {"id": i, "data": f"item_{i}"}

# Define processor function
def process_item(item):
    return {
        "id": item["id"],
        "processed_data": item["data"].upper(),
        "timestamp": time.time()
    }

# Process stream
async def main():
    await processor.start()
    
    async for result in processor.process_stream(data_source(), process_item):
        print(f"Processed: {result}")
    
    await processor.stop()

asyncio.run(main())
```

### Reactive Transformation

```python
from reactive import ReactiveTransformer, StreamConfig, BackpressureConfig, TransformationConfig

# Configure components
stream_config = StreamConfig(buffer_size=500, batch_size=50)
backpressure_config = BackpressureConfig(strategy=BackpressureStrategy.ADAPTIVE)
transformation_config = TransformationConfig(
    max_workers=4,
    enable_caching=True,
    enable_validation=True
)

transformer = ReactiveTransformer(
    stream_config, 
    backpressure_config, 
    transformation_config
)

# Define transformation pipeline
async def transform_data():
    async def source():
        for i in range(10000):
            yield {"user_id": i, "data": f"user_data_{i}"}
    
    def transform_user(user):
        return {
            "user_id": user["user_id"],
            "transformed_data": user["data"].replace("_", "-"),
            "processed_at": time.time()
        }
    
    def validate_user(user):
        return "user_id" in user and "transformed_data" in user
    
    async for result in transformer.transform_stream(
        source(), 
        transform_user, 
        validate_user
    ):
        print(f"Transformed user: {result}")

asyncio.run(transform_data())
```

### Stream Operators Chain

```python
from reactive import StreamOperators

async def process_with_operators():
    # Create data source
    async def numbers():
        for i in range(100):
            yield i
    
    # Chain operators
    doubled = StreamOperators.map(numbers(), lambda x: x * 2)
    evens = StreamOperators.filter(doubled, lambda x: x % 4 == 0)
    batched = StreamOperators.batch(evens, 10)
    limited = StreamOperators.take(batched, 5)
    
    # Process results
    async for batch in limited:
        print(f"Batch: {batch}")

asyncio.run(process_with_operators())
```

### Flow Control

```python
from reactive import FlowController, FlowControlConfig, CongestionAlgorithm

# Configure flow controller
config = FlowControlConfig(
    initial_rate=100.0,
    max_rate=1000.0,
    congestion_algorithm=CongestionAlgorithm.ADAPTIVE,
    enable_prediction=True
)

flow_controller = FlowController(config)

async def adaptive_processing():
    for i in range(1000):
        # Simulate processing metrics
        queue_length = random.randint(0, 50)
        processing_latency = random.uniform(0.001, 0.1)
        throughput = random.uniform(50, 150)
        
        # Get recommended rate
        recommended_rate = await flow_controller.control_flow(
            queue_length, 
            processing_latency, 
            throughput
        )
        
        print(f"Recommended rate: {recommended_rate:.2f} items/sec")
        
        # Apply rate limiting
        await asyncio.sleep(1.0 / recommended_rate)

asyncio.run(adaptive_processing())
```

## Configuration

### StreamConfig

```python
@dataclass
class StreamConfig:
    buffer_size: int = 1000                    # Internal buffer size
    max_concurrent_operations: int = 10        # Max concurrent processing
    backpressure_threshold: float = 0.8        # Backpressure trigger threshold
    batch_size: int = 100                      # Processing batch size
    processing_timeout: float = 30.0           # Processing timeout
    enable_metrics: bool = True                # Enable metrics collection
    enable_flow_control: bool = True           # Enable flow control
    max_retry_attempts: int = 3                # Max retry attempts
    retry_delay: float = 1.0                   # Retry delay
```

### BackpressureConfig

```python
@dataclass
class BackpressureConfig:
    strategy: BackpressureStrategy = BackpressureStrategy.ADAPTIVE
    buffer_size: int = 1000                    # Buffer size
    high_watermark: float = 0.8                # High watermark threshold
    low_watermark: float = 0.3                 # Low watermark threshold
    sample_rate: float = 0.1                   # Sampling rate (for SAMPLE strategy)
    adaptive_window: int = 100                 # Adaptive calculation window
    circuit_breaker_threshold: int = 5         # Circuit breaker threshold
    recovery_timeout: float = 30.0             # Recovery timeout
```

### FlowControlConfig

```python
@dataclass
class FlowControlConfig:
    initial_rate: float = 100.0                # Initial processing rate
    max_rate: float = 1000.0                   # Maximum rate
    min_rate: float = 1.0                      # Minimum rate
    congestion_algorithm: CongestionAlgorithm = CongestionAlgorithm.ADAPTIVE
    congestion_threshold: float = 0.8          # Congestion threshold
    recovery_factor: float = 0.5               # Recovery factor
    increase_factor: float = 1.1               # Increase factor
    measurement_window: int = 100              # Measurement window
    enable_prediction: bool = True             # Enable predictive control
    prediction_horizon: int = 10               # Prediction horizon
```

## Advanced Usage

### Custom Backpressure Strategy

```python
class CustomBackpressureHandler(BackpressureHandler):
    async def _apply_custom_strategy(self, item, processor, buffer_usage):
        # Implement custom backpressure logic
        if buffer_usage > 0.9:
            # Critical level - aggressive dropping
            if random.random() < 0.5:
                return None  # Drop item
        elif buffer_usage > 0.7:
            # High level - selective processing
            if item.get("priority", 0) < 5:
                return None  # Drop low priority items
        
        return await self._process_normally(item, processor)
```

### Custom Flow Control Algorithm

```python
class MLFlowController(FlowController):
    def __init__(self, config, ml_model=None):
        super().__init__(config)
        self.ml_model = ml_model
    
    async def _apply_ml_prediction(self):
        if self.ml_model and len(self.adaptive_history) >= 20:
            # Use ML model to predict optimal rate
            features = self._extract_features()
            predicted_rate = self.ml_model.predict(features)
            self.target_rate = max(
                self.config.min_rate,
                min(self.config.max_rate, predicted_rate)
            )
```

### Stream Operator Composition

```python
async def complex_pipeline(source):
    # Create a complex processing pipeline
    pipeline = (
        source
        |> StreamOperators.map(lambda x: x * 2)
        |> StreamOperators.filter(lambda x: x > 10)
        |> StreamOperators.batch(20)
        |> StreamOperators.flat_map(lambda batch: process_batch_async(batch))
        |> StreamOperators.distinct()
        |> StreamOperators.throttle(0.1)
        |> StreamOperators.take(1000)
    )
    
    async for result in pipeline:
        yield result
```

## Performance Optimization

### Buffer Sizing

```python
# For high-throughput scenarios
config = StreamConfig(
    buffer_size=10000,           # Large buffer
    max_concurrent_operations=20, # High concurrency
    batch_size=500               # Large batches
)

# For low-latency scenarios
config = StreamConfig(
    buffer_size=100,             # Small buffer
    max_concurrent_operations=5, # Lower concurrency
    batch_size=10                # Small batches
)
```

### Memory Management

```python
# Enable caching for repeated transformations
transformation_config = TransformationConfig(
    enable_caching=True,
    cache_size=10000,
    max_workers=cpu_count()
)

# Disable caching for unique data
transformation_config = TransformationConfig(
    enable_caching=False,
    max_workers=cpu_count() * 2
)
```

### Backpressure Tuning

```python
# For stable, predictable loads
backpressure_config = BackpressureConfig(
    strategy=BackpressureStrategy.BLOCK,
    high_watermark=0.8,
    low_watermark=0.3
)

# For variable, bursty loads
backpressure_config = BackpressureConfig(
    strategy=BackpressureStrategy.ADAPTIVE,
    high_watermark=0.7,
    low_watermark=0.2
)
```

## Monitoring and Metrics

### Stream Processor Metrics

```python
metrics = stream_processor.get_metrics_summary()
print(f"Items processed: {metrics['items_processed']}")
print(f"Items failed: {metrics['items_failed']}")
print(f"Throughput: {metrics['throughput_per_second']} items/sec")
print(f"Average latency: {metrics['average_processing_time_ms']} ms")
print(f"Buffer usage: {metrics['buffer_usage_percent']}%")
```

### Backpressure Metrics

```python
bp_metrics = backpressure_handler.get_metrics()
print(f"Strategy: {bp_metrics['strategy']}")
print(f"Dropped items: {bp_metrics['dropped_items']}")
print(f"Blocked time: {bp_metrics['blocked_time_seconds']} sec")
print(f"Circuit open: {bp_metrics['circuit_open']}")
```

### Flow Control Metrics

```python
flow_metrics = flow_controller.get_metrics()
print(f"Current rate: {flow_metrics['current_rate']} items/sec")
print(f"Congestion events: {flow_metrics['congestion_events']}")
print(f"Bandwidth utilization: {flow_metrics['bandwidth_utilization']}")
print(f"Efficiency: {flow_metrics['efficiency']}")
```

## Error Handling

### Retry Logic

```python
# Configure retry behavior
config = StreamConfig(
    max_retry_attempts=3,
    retry_delay=1.0,
    processing_timeout=30.0
)

# Custom error handler
def handle_processing_error(error):
    logger.error(f"Processing error: {error}")
    # Custom error handling logic

stream_processor.add_error_handler(handle_processing_error)
```

### Circuit Breaker

```python
# Configure circuit breaker
backpressure_config = BackpressureConfig(
    strategy=BackpressureStrategy.CIRCUIT_BREAKER,
    circuit_breaker_threshold=5,
    recovery_timeout=30.0
)

# Monitor circuit breaker state
if backpressure_handler.get_metrics()["circuit_open"]:
    logger.warning("Circuit breaker is open - system overloaded")
```

## Integration with Migration Tool

### Migration Data Processing

```python
from reactive import ReactiveTransformer
from transformer.data_transformer import DataTransformer

class ReactiveMigrationProcessor:
    def __init__(self):
        self.reactive_transformer = ReactiveTransformer(
            stream_config=StreamConfig(buffer_size=5000),
            backpressure_config=BackpressureConfig(
                strategy=BackpressureStrategy.ADAPTIVE
            )
        )
        self.data_transformer = DataTransformer()
    
    async def migrate_users(self, user_stream):
        async for transformed_user in self.reactive_transformer.transform_stream(
            user_stream,
            self.data_transformer.transform_user,
            self.data_transformer.validate_user
        ):
            yield transformed_user
```

### Event-Driven Processing

```python
from events import EventBus
from reactive import StreamProcessor

class ReactiveEventProcessor:
    def __init__(self, event_bus: EventBus):
        self.event_bus = event_bus
        self.stream_processor = StreamProcessor()
    
    async def process_migration_events(self):
        async def event_stream():
            async for event in self.event_bus.subscribe("migration.*"):
                yield event
        
        async for processed_event in self.stream_processor.process_stream(
            event_stream(),
            self.process_event
        ):
            await self.event_bus.publish(processed_event)
```

## Best Practices

### 1. Choose Appropriate Buffer Sizes
- **Small buffers** (100-1000): Low latency, limited throughput
- **Medium buffers** (1000-10000): Balanced latency/throughput
- **Large buffers** (10000+): High throughput, higher latency

### 2. Select Backpressure Strategy
- **BLOCK**: Predictable loads, guaranteed processing
- **DROP**: High-volume, loss-tolerant scenarios
- **SAMPLE**: Real-time analytics, approximate results
- **ADAPTIVE**: Variable loads, optimal performance

### 3. Configure Flow Control
- **AIMD**: Simple, well-tested algorithm
- **CUBIC**: High-bandwidth, long-delay networks
- **BBR**: Modern, bandwidth-optimal algorithm
- **ADAPTIVE**: Machine learning-based optimization

### 4. Monitor Performance
- Track throughput and latency metrics
- Monitor buffer utilization
- Watch for backpressure events
- Observe congestion patterns

### 5. Handle Errors Gracefully
- Implement comprehensive error handlers
- Use circuit breakers for fault tolerance
- Configure appropriate retry policies
- Log errors for debugging

## Testing

### Unit Tests

```bash
# Run reactive streams tests
python -m pytest tests/test_reactive_streams.py -v

# Run specific test categories
python -m pytest tests/test_reactive_streams.py::TestStreamProcessor -v
python -m pytest tests/test_reactive_streams.py::TestBackpressureHandler -v
python -m pytest tests/test_reactive_streams.py::TestFlowController -v
```

### Performance Tests

```python
import asyncio
import time
from reactive import StreamProcessor, StreamConfig

async def performance_test():
    config = StreamConfig(
        buffer_size=10000,
        max_concurrent_operations=20,
        batch_size=1000
    )
    
    processor = StreamProcessor(config)
    
    async def large_data_source():
        for i in range(100000):
            yield {"id": i, "data": f"item_{i}"}
    
    def simple_processor(item):
        return item["data"].upper()
    
    start_time = time.time()
    count = 0
    
    await processor.start()
    
    async for result in processor.process_stream(large_data_source(), simple_processor):
        count += 1
    
    await processor.stop()
    
    elapsed = time.time() - start_time
    throughput = count / elapsed
    
    print(f"Processed {count} items in {elapsed:.2f} seconds")
    print(f"Throughput: {throughput:.2f} items/second")

asyncio.run(performance_test())
```

## Conclusion

The reactive streams implementation provides a powerful, scalable foundation for high-performance data processing in the migration tool. With automatic backpressure handling, sophisticated flow control, and comprehensive monitoring, it enables efficient processing of large-scale migration workloads while maintaining system stability and optimal resource utilization.

Key benefits:
- **High Performance**: Optimized for throughput and low latency
- **Fault Tolerance**: Comprehensive error handling and recovery
- **Scalability**: Handles varying loads with adaptive algorithms
- **Monitoring**: Real-time metrics and performance insights
- **Flexibility**: Configurable strategies and operators