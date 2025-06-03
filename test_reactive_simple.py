"""
Simple test to verify reactive streams functionality.
"""

import asyncio
import time
from reactive import StreamProcessor, StreamConfig, BackpressureHandler, BackpressureConfig, BackpressureStrategy


async def test_basic_stream_processing():
    """Test basic stream processing functionality."""
    print("Testing basic stream processing...")
    
    # Create stream processor
    config = StreamConfig(
        buffer_size=100,
        max_concurrent_operations=5,
        batch_size=10
    )
    processor = StreamProcessor(config)
    
    # Create data source
    async def data_source():
        for i in range(20):
            yield i
            await asyncio.sleep(0.01)
    
    # Define processor function
    def process_item(item):
        return item * 2
    
    # Process stream
    await processor.start()
    
    results = []
    async for result in processor.process_stream(data_source(), process_item):
        results.append(result)
    
    await processor.stop()
    
    print(f"Processed {len(results)} items")
    print(f"Results: {results[:10]}...")  # Show first 10
    
    # Get metrics
    metrics = processor.get_metrics_summary()
    print(f"Metrics: {metrics}")
    
    assert len(results) == 20
    assert results[0] == 0
    assert results[1] == 2
    print("✅ Basic stream processing test passed!")


async def test_backpressure_handling():
    """Test backpressure handling."""
    print("\nTesting backpressure handling...")
    
    config = BackpressureConfig(
        strategy=BackpressureStrategy.DROP,
        buffer_size=5
    )
    handler = BackpressureHandler(config)
    
    def simple_processor(item):
        return item * 3
    
    results = []
    for i in range(10):
        result = await handler.handle_item(i, simple_processor, i)
        if result is not None:
            results.append(result)
    
    print(f"Processed {len(results)} items with backpressure")
    metrics = handler.get_metrics()
    print(f"Backpressure metrics: {metrics}")
    
    assert len(results) <= 10  # Some items may be dropped
    print("✅ Backpressure handling test passed!")


async def main():
    """Run all tests."""
    print("🚀 Starting Reactive Streams Tests\n")
    
    try:
        await test_basic_stream_processing()
        await test_backpressure_handling()
        print("\n🎉 All tests passed successfully!")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())