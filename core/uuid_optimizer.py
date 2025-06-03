# migration/core/uuid_optimizer.py

import re
import time
import uuid
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, List, Optional, Tuple, Set, Any
from dataclasses import dataclass
from enum import Enum

from core.logging_setup import get_logger

logger = get_logger()


class UUIDFormat(Enum):
    """Supported UUID formats for transformation."""
    HEX_32 = "hex_32"  # 32-character hex string without dashes
    UUID_STANDARD = "uuid_standard"  # Standard UUID format with dashes
    UUID_COMPACT = "uuid_compact"  # UUID without dashes but with proper validation
    INVALID = "invalid"


@dataclass
class UUIDTransformationResult:
    """Result of UUID transformation operation."""
    original: str
    transformed: Optional[str]
    format_detected: UUIDFormat
    is_valid: bool
    error_message: Optional[str] = None


@dataclass
class BatchTransformationStats:
    """Statistics for batch UUID transformation."""
    total_processed: int
    successful_transformations: int
    failed_transformations: int
    processing_time_ms: float
    throughput_per_second: float
    format_distribution: Dict[UUIDFormat, int]


class UUIDOptimizer:
    """Optimized UUID transformation engine with batch processing and fallback mechanisms."""
    
    # Pre-compiled regex patterns for performance
    HEX_32_PATTERN = re.compile(r'^[0-9a-fA-F]{32}$')
    UUID_PATTERN = re.compile(r'^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$')
    UUID_COMPACT_PATTERN = re.compile(r'^[0-9a-fA-F]{32}$')
    
    def __init__(self, max_workers: int = 4, enable_validation: bool = True):
        """
        Initialize UUID optimizer.
        
        Args:
            max_workers: Maximum number of threads for batch processing
            enable_validation: Whether to perform strict UUID validation
        """
        self.max_workers = max_workers
        self.enable_validation = enable_validation
        self._transformation_cache: Dict[str, UUIDTransformationResult] = {}
        
    def detect_uuid_format(self, uuid_str: str) -> UUIDFormat:
        """
        Detect the format of a UUID string.
        
        Args:
            uuid_str: The UUID string to analyze
            
        Returns:
            UUIDFormat enum indicating the detected format
        """
        if not isinstance(uuid_str, str):
            return UUIDFormat.INVALID
            
        uuid_str = uuid_str.strip()
        
        if self.UUID_PATTERN.match(uuid_str):
            return UUIDFormat.UUID_STANDARD
        elif self.HEX_32_PATTERN.match(uuid_str):
            return UUIDFormat.HEX_32
        elif len(uuid_str) == 32 and self.UUID_COMPACT_PATTERN.match(uuid_str):
            return UUIDFormat.UUID_COMPACT
        else:
            return UUIDFormat.INVALID
    
    def transform_single_uuid(self, uuid_str: str, target_format: UUIDFormat = UUIDFormat.UUID_STANDARD) -> UUIDTransformationResult:
        """
        Transform a single UUID to the target format.
        
        Args:
            uuid_str: The UUID string to transform
            target_format: The desired output format
            
        Returns:
            UUIDTransformationResult with transformation details
        """
        # Check cache first
        cache_key = f"{uuid_str}:{target_format.value}"
        if cache_key in self._transformation_cache:
            return self._transformation_cache[cache_key]
        
        detected_format = self.detect_uuid_format(uuid_str)
        
        if detected_format == UUIDFormat.INVALID:
            result = UUIDTransformationResult(
                original=uuid_str,
                transformed=None,
                format_detected=detected_format,
                is_valid=False,
                error_message="Invalid UUID format detected"
            )
            self._transformation_cache[cache_key] = result
            return result
        
        try:
            transformed = self._perform_transformation(uuid_str, detected_format, target_format)
            
            # Validate the result if enabled
            is_valid = True
            error_message = None
            
            if self.enable_validation and transformed:
                is_valid = self._validate_uuid(transformed, target_format)
                if not is_valid:
                    error_message = f"Transformed UUID failed validation: {transformed}"
            
            result = UUIDTransformationResult(
                original=uuid_str,
                transformed=transformed,
                format_detected=detected_format,
                is_valid=is_valid,
                error_message=error_message
            )
            
            self._transformation_cache[cache_key] = result
            return result
            
        except Exception as e:
            result = UUIDTransformationResult(
                original=uuid_str,
                transformed=None,
                format_detected=detected_format,
                is_valid=False,
                error_message=f"Transformation error: {str(e)}"
            )
            self._transformation_cache[cache_key] = result
            return result
    
    def _perform_transformation(self, uuid_str: str, source_format: UUIDFormat, target_format: UUIDFormat) -> Optional[str]:
        """
        Perform the actual UUID transformation.
        
        Args:
            uuid_str: The UUID string to transform
            source_format: The detected source format
            target_format: The desired target format
            
        Returns:
            Transformed UUID string or None if transformation fails
        """
        if source_format == target_format:
            return uuid_str
        
        # Normalize to hex32 first
        if source_format == UUIDFormat.UUID_STANDARD:
            hex32 = uuid_str.replace('-', '')
        elif source_format in [UUIDFormat.HEX_32, UUIDFormat.UUID_COMPACT]:
            hex32 = uuid_str
        else:
            return None
        
        # Transform to target format
        if target_format == UUIDFormat.UUID_STANDARD:
            return f"{hex32[:8]}-{hex32[8:12]}-{hex32[12:16]}-{hex32[16:20]}-{hex32[20:]}"
        elif target_format == UUIDFormat.HEX_32:
            return hex32
        elif target_format == UUIDFormat.UUID_COMPACT:
            return hex32
        else:
            return None
    
    def _validate_uuid(self, uuid_str: str, expected_format: UUIDFormat) -> bool:
        """
        Validate a UUID string against the expected format.
        
        Args:
            uuid_str: The UUID string to validate
            expected_format: The expected format
            
        Returns:
            True if valid, False otherwise
        """
        try:
            if expected_format == UUIDFormat.UUID_STANDARD:
                # Try to parse as standard UUID
                uuid.UUID(uuid_str)
                return True
            elif expected_format in [UUIDFormat.HEX_32, UUIDFormat.UUID_COMPACT]:
                # Validate as hex string and try to create UUID
                if len(uuid_str) == 32 and self.HEX_32_PATTERN.match(uuid_str):
                    # Try to create a UUID from the hex string
                    formatted = f"{uuid_str[:8]}-{uuid_str[8:12]}-{uuid_str[12:16]}-{uuid_str[16:20]}-{uuid_str[20:]}"
                    uuid.UUID(formatted)
                    return True
            return False
        except (ValueError, TypeError):
            return False
    
    def transform_batch(self, uuid_list: List[str], target_format: UUIDFormat = UUIDFormat.UUID_STANDARD) -> Tuple[List[UUIDTransformationResult], BatchTransformationStats]:
        """
        Transform a batch of UUIDs with optimized parallel processing.
        
        Args:
            uuid_list: List of UUID strings to transform
            target_format: The desired output format for all UUIDs
            
        Returns:
            Tuple of (transformation results, batch statistics)
        """
        start_time = time.time()
        results: List[UUIDTransformationResult] = []
        format_distribution: Dict[UUIDFormat, int] = {fmt: 0 for fmt in UUIDFormat}
        
        # Remove duplicates while preserving order
        unique_uuids = list(dict.fromkeys(uuid_list))
        
        if len(unique_uuids) <= 10 or self.max_workers == 1:
            # Process sequentially for small batches
            for uuid_str in unique_uuids:
                result = self.transform_single_uuid(uuid_str, target_format)
                results.append(result)
                format_distribution[result.format_detected] += 1
        else:
            # Process in parallel for larger batches
            with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                future_to_uuid = {
                    executor.submit(self.transform_single_uuid, uuid_str, target_format): uuid_str
                    for uuid_str in unique_uuids
                }
                
                for future in as_completed(future_to_uuid):
                    result = future.result()
                    results.append(result)
                    format_distribution[result.format_detected] += 1
        
        # Sort results to match original order
        uuid_to_result = {result.original: result for result in results}
        ordered_results = [uuid_to_result[uuid_str] for uuid_str in unique_uuids]
        
        # Calculate statistics
        end_time = time.time()
        processing_time_ms = (end_time - start_time) * 1000
        successful_count = sum(1 for r in ordered_results if r.is_valid)
        failed_count = len(ordered_results) - successful_count
        throughput = len(ordered_results) / (processing_time_ms / 1000) if processing_time_ms > 0 else 0
        
        stats = BatchTransformationStats(
            total_processed=len(ordered_results),
            successful_transformations=successful_count,
            failed_transformations=failed_count,
            processing_time_ms=processing_time_ms,
            throughput_per_second=throughput,
            format_distribution=format_distribution
        )
        
        logger.info(f"Batch UUID transformation completed: {successful_count}/{len(ordered_results)} successful, "
                   f"{processing_time_ms:.2f}ms, {throughput:.1f} UUIDs/sec")
        
        return ordered_results, stats
    
    def get_transformation_recommendations(self, uuid_list: List[str]) -> Dict[str, Any]:
        """
        Analyze a list of UUIDs and provide transformation recommendations.
        
        Args:
            uuid_list: List of UUID strings to analyze
            
        Returns:
            Dictionary with analysis results and recommendations
        """
        format_counts: Dict[UUIDFormat, int] = {fmt: 0 for fmt in UUIDFormat}
        invalid_uuids: List[str] = []
        
        for uuid_str in uuid_list:
            detected_format = self.detect_uuid_format(uuid_str)
            format_counts[detected_format] += 1
            
            if detected_format == UUIDFormat.INVALID:
                invalid_uuids.append(uuid_str)
        
        total_count = len(uuid_list)
        recommendations = []
        
        # Analyze format distribution
        if format_counts[UUIDFormat.HEX_32] > 0:
            percentage = (format_counts[UUIDFormat.HEX_32] / total_count) * 100
            recommendations.append(f"Found {format_counts[UUIDFormat.HEX_32]} hex32 UUIDs ({percentage:.1f}%) - recommend transformation to standard format")
        
        if format_counts[UUIDFormat.INVALID] > 0:
            percentage = (format_counts[UUIDFormat.INVALID] / total_count) * 100
            recommendations.append(f"Found {format_counts[UUIDFormat.INVALID]} invalid UUIDs ({percentage:.1f}%) - require manual review")
        
        if format_counts[UUIDFormat.UUID_STANDARD] == total_count:
            recommendations.append("All UUIDs are already in standard format - no transformation needed")
        
        return {
            "total_uuids": total_count,
            "format_distribution": format_counts,
            "invalid_uuids": invalid_uuids[:10],  # Show first 10 invalid UUIDs
            "invalid_count": len(invalid_uuids),
            "recommendations": recommendations,
            "estimated_processing_time_ms": self._estimate_processing_time(total_count),
            "recommended_batch_size": self._recommend_batch_size(total_count)
        }
    
    def _estimate_processing_time(self, uuid_count: int) -> float:
        """Estimate processing time for a given number of UUIDs."""
        # Based on empirical testing: ~0.1ms per UUID for small batches, ~0.05ms for large batches
        if uuid_count <= 100:
            return uuid_count * 0.1
        else:
            return uuid_count * 0.05
    
    def _recommend_batch_size(self, total_count: int) -> int:
        """Recommend optimal batch size based on total count."""
        if total_count <= 100:
            return total_count
        elif total_count <= 1000:
            return 100
        else:
            return 500
    
    def clear_cache(self):
        """Clear the transformation cache."""
        self._transformation_cache.clear()
        logger.debug("UUID transformation cache cleared")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get statistics about the transformation cache."""
        return {
            "cache_size": len(self._transformation_cache),
            "cache_hit_ratio": "N/A",  # Would need to track hits/misses
            "memory_usage_estimate": len(self._transformation_cache) * 200  # Rough estimate in bytes
        }