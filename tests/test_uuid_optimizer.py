# migration/tests/test_uuid_optimizer.py

import unittest
import time
from unittest.mock import patch, MagicMock

from core.uuid_optimizer import UUIDOptimizer, UUIDFormat, UUIDTransformationResult, BatchTransformationStats


class TestUUIDOptimizer(unittest.TestCase):
    """Test cases for UUID optimization functionality."""
    
    def setUp(self):
        """Set up test fixtures."""
        self.optimizer = UUIDOptimizer(max_workers=2, enable_validation=True)
        
        # Test UUIDs in different formats
        self.valid_hex32 = "550e8400e29b41d4a716446655440000"
        self.valid_uuid_standard = "550e8400-e29b-41d4-a716-446655440000"
        self.invalid_uuid = "invalid-uuid-string"
        self.short_hex = "550e8400"
        
    def test_detect_uuid_format(self):
        """Test UUID format detection."""
        # Test valid formats
        self.assertEqual(
            self.optimizer.detect_uuid_format(self.valid_hex32),
            UUIDFormat.HEX_32
        )
        
        self.assertEqual(
            self.optimizer.detect_uuid_format(self.valid_uuid_standard),
            UUIDFormat.UUID_STANDARD
        )
        
        # Test invalid formats
        self.assertEqual(
            self.optimizer.detect_uuid_format(self.invalid_uuid),
            UUIDFormat.INVALID
        )
        
        self.assertEqual(
            self.optimizer.detect_uuid_format(self.short_hex),
            UUIDFormat.INVALID
        )
        
        # Test edge cases
        self.assertEqual(
            self.optimizer.detect_uuid_format(""),
            UUIDFormat.INVALID
        )
        
        self.assertEqual(
            self.optimizer.detect_uuid_format(None),
            UUIDFormat.INVALID
        )
    
    def test_transform_single_uuid_hex32_to_standard(self):
        """Test transforming hex32 to standard UUID format."""
        result = self.optimizer.transform_single_uuid(
            self.valid_hex32, 
            UUIDFormat.UUID_STANDARD
        )
        
        self.assertTrue(result.is_valid)
        self.assertEqual(result.format_detected, UUIDFormat.HEX_32)
        self.assertEqual(result.transformed, self.valid_uuid_standard)
        self.assertIsNone(result.error_message)
    
    def test_transform_single_uuid_invalid(self):
        """Test transforming invalid UUID."""
        result = self.optimizer.transform_single_uuid(
            self.invalid_uuid,
            UUIDFormat.UUID_STANDARD
        )
        
        self.assertFalse(result.is_valid)
        self.assertEqual(result.format_detected, UUIDFormat.INVALID)
        self.assertIsNone(result.transformed)
        self.assertIsNotNone(result.error_message)
    
    def test_transform_batch_mixed_formats(self):
        """Test batch transformation with mixed UUID formats."""
        uuid_list = [
            self.valid_hex32,
            self.valid_uuid_standard,
            self.invalid_uuid,
            "123e4567e89b12d3a456426614174000"  # Another hex32
        ]
        
        results, stats = self.optimizer.transform_batch(
            uuid_list, 
            UUIDFormat.UUID_STANDARD
        )
        
        # Check results
        self.assertEqual(len(results), 4)
        
        # First UUID (hex32) should be transformed
        self.assertTrue(results[0].is_valid)
        self.assertEqual(results[0].transformed, self.valid_uuid_standard)
        
        # Second UUID (already standard) should remain the same
        self.assertTrue(results[1].is_valid)
        self.assertEqual(results[1].transformed, self.valid_uuid_standard)
        
        # Third UUID (invalid) should fail
        self.assertFalse(results[2].is_valid)
        
        # Fourth UUID (hex32) should be transformed
        self.assertTrue(results[3].is_valid)
        self.assertEqual(results[3].transformed, "123e4567-e89b-12d3-a456-426614174000")
        
        # Check statistics
        self.assertEqual(stats.total_processed, 4)
        self.assertEqual(stats.successful_transformations, 3)
        self.assertEqual(stats.failed_transformations, 1)
        self.assertGreater(stats.throughput_per_second, 0)


if __name__ == "__main__":
    unittest.main()