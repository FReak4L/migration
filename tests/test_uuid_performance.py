# migration/tests/test_uuid_performance.py

import unittest
import time
from unittest.mock import MagicMock

from core.uuid_optimizer import UUIDOptimizer, UUIDFormat
from transformer.data_transformer import DataTransformer


class TestUUIDPerformance(unittest.TestCase):
    """Performance benchmarks for UUID optimization."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.optimizer = UUIDOptimizer(max_workers=4, enable_validation=True)
        mock_config = MagicMock()
        self.transformer = DataTransformer(mock_config)
        
    def test_single_uuid_performance(self):
        """Benchmark single UUID transformation performance."""
        test_uuid = "550e8400e29b41d4a716446655440000"
        iterations = 1000
        
        start_time = time.time()
        for _ in range(iterations):
            result = self.optimizer.transform_single_uuid(test_uuid, UUIDFormat.UUID_STANDARD)
            self.assertTrue(result.is_valid)
        end_time = time.time()
        
        total_time = end_time - start_time
        avg_time_ms = (total_time / iterations) * 1000
        throughput = iterations / total_time
        
        print(f"\nSingle UUID Performance:")
        print(f"  Iterations: {iterations}")
        print(f"  Total time: {total_time:.3f}s")
        print(f"  Average time per UUID: {avg_time_ms:.3f}ms")
        print(f"  Throughput: {throughput:.1f} UUIDs/sec")
        
        # Performance assertions
        self.assertLess(avg_time_ms, 1.0, "Single UUID transformation should be under 1ms")
        self.assertGreater(throughput, 1000, "Should process at least 1000 UUIDs per second")
    
    def test_batch_uuid_performance(self):
        """Benchmark batch UUID transformation performance."""
        # Generate test UUIDs
        test_uuids = [f"{i:032x}" for i in range(1000)]
        
        start_time = time.time()
        results, stats = self.optimizer.transform_batch(test_uuids, UUIDFormat.UUID_STANDARD)
        end_time = time.time()
        
        total_time = end_time - start_time
        
        print(f"\nBatch UUID Performance:")
        print(f"  Batch size: {len(test_uuids)}")
        print(f"  Total time: {total_time:.3f}s")
        print(f"  Successful transformations: {stats.successful_transformations}")
        print(f"  Failed transformations: {stats.failed_transformations}")
        print(f"  Throughput: {stats.throughput_per_second:.1f} UUIDs/sec")
        print(f"  Processing time (internal): {stats.processing_time_ms:.2f}ms")
        
        # Performance assertions
        self.assertEqual(len(results), 1000)
        self.assertEqual(stats.successful_transformations, 1000)
        self.assertEqual(stats.failed_transformations, 0)
        self.assertGreater(stats.throughput_per_second, 5000, "Batch processing should be faster than 5000 UUIDs/sec")
        self.assertLess(total_time, 1.0, "1000 UUIDs should process in under 1 second")
    
    def test_caching_performance_improvement(self):
        """Test that caching improves performance for repeated UUIDs."""
        test_uuid = "550e8400e29b41d4a716446655440000"
        iterations = 100
        
        # First run (no cache)
        self.optimizer.clear_cache()
        start_time = time.time()
        for _ in range(iterations):
            self.optimizer.transform_single_uuid(test_uuid, UUIDFormat.UUID_STANDARD)
        first_run_time = time.time() - start_time
        
        # Second run (with cache)
        start_time = time.time()
        for _ in range(iterations):
            self.optimizer.transform_single_uuid(test_uuid, UUIDFormat.UUID_STANDARD)
        second_run_time = time.time() - start_time
        
        print(f"\nCaching Performance:")
        print(f"  First run (no cache): {first_run_time:.3f}s")
        print(f"  Second run (cached): {second_run_time:.3f}s")
        print(f"  Improvement factor: {first_run_time / second_run_time:.1f}x")
        
        # Cache should provide some improvement
        self.assertLessEqual(second_run_time, first_run_time, "Cached run should be faster or equal")
    
    def test_parallel_vs_sequential_performance(self):
        """Compare parallel vs sequential processing performance."""
        test_uuids = [f"{i:032x}" for i in range(500)]
        
        # Sequential processing
        optimizer_sequential = UUIDOptimizer(max_workers=1)
        start_time = time.time()
        results_seq, stats_seq = optimizer_sequential.transform_batch(test_uuids)
        sequential_time = time.time() - start_time
        
        # Parallel processing
        optimizer_parallel = UUIDOptimizer(max_workers=4)
        start_time = time.time()
        results_par, stats_par = optimizer_parallel.transform_batch(test_uuids)
        parallel_time = time.time() - start_time
        
        print(f"\nParallel vs Sequential Performance:")
        print(f"  Sequential time: {sequential_time:.3f}s ({stats_seq.throughput_per_second:.1f} UUIDs/sec)")
        print(f"  Parallel time: {parallel_time:.3f}s ({stats_par.throughput_per_second:.1f} UUIDs/sec)")
        print(f"  Speedup: {sequential_time / parallel_time:.1f}x")
        
        # Both should produce same results
        self.assertEqual(len(results_seq), len(results_par))
        self.assertEqual(stats_seq.successful_transformations, stats_par.successful_transformations)
    
    def test_memory_usage_stability(self):
        """Test that memory usage remains stable during large batch processing."""
        import gc
        
        # Process multiple large batches
        batch_size = 1000
        num_batches = 5
        
        for batch_num in range(num_batches):
            test_uuids = [f"{i + batch_num * batch_size:032x}" for i in range(batch_size)]
            
            results, stats = self.optimizer.transform_batch(test_uuids)
            
            # Verify processing
            self.assertEqual(len(results), batch_size)
            self.assertEqual(stats.successful_transformations, batch_size)
            
            # Force garbage collection
            gc.collect()
            
            print(f"Batch {batch_num + 1}/{num_batches}: {stats.throughput_per_second:.1f} UUIDs/sec")
        
        # Clear cache and verify it's empty
        cache_stats_before = self.optimizer.get_cache_stats()
        self.optimizer.clear_cache()
        cache_stats_after = self.optimizer.get_cache_stats()
        
        print(f"\nMemory Management:")
        print(f"  Cache size before clear: {cache_stats_before['cache_size']}")
        print(f"  Cache size after clear: {cache_stats_after['cache_size']}")
        
        self.assertEqual(cache_stats_after['cache_size'], 0)
    
    def test_data_transformer_integration_performance(self):
        """Test performance of UUID optimization in DataTransformer."""
        test_uuids = [f"{i:032x}" for i in range(500)]
        
        start_time = time.time()
        optimized_uuids, stats = self.transformer.optimize_uuid_batch(test_uuids)
        end_time = time.time()
        
        total_time = end_time - start_time
        
        print(f"\nDataTransformer Integration Performance:")
        print(f"  Input UUIDs: {len(test_uuids)}")
        print(f"  Output UUIDs: {len(optimized_uuids)}")
        print(f"  Total time: {total_time:.3f}s")
        print(f"  Successful optimizations: {stats['successful_optimizations']}")
        print(f"  Throughput: {stats['throughput_per_second']:.1f} UUIDs/sec")
        
        # Verify all UUIDs were processed
        self.assertEqual(len(optimized_uuids), len(test_uuids))
        self.assertEqual(stats['successful_optimizations'], len(test_uuids))
        
        # Verify UUIDs are in standard format
        for uuid_str in optimized_uuids:
            self.assertIn('-', uuid_str, "Output UUIDs should be in standard format")
            self.assertEqual(len(uuid_str), 36, "Standard UUID should be 36 characters")


if __name__ == "__main__":
    unittest.main(verbosity=2)