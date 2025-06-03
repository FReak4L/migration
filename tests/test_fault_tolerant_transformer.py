"""
Tests for Fault Tolerant DataTransformer.
"""

import os
# Set environment variables before importing config
os.environ.update({
    'MARZNESHIN_API_URL': 'http://test-marzneshin.com',
    'MARZNESHIN_USERNAME': 'test_user',
    'MARZNESHIN_PASSWORD': 'test_pass',
    'MARZBAN_API_URL': 'http://test-marzban.com',
    'MARZBAN_USERNAME': 'test_user',
    'MARZBAN_PASSWORD': 'test_pass'
})

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime

from core.config import AppConfig
from transformer.data_transformer import DataTransformer
from core.fault_tolerance import RetryConfig


class TestFaultTolerantTransformer:
    """Test cases for fault tolerant DataTransformer."""
    
    @pytest.fixture
    def config(self):
        """App configuration for testing."""
        return AppConfig()
    
    @pytest.fixture
    def transformer(self, config):
        """DataTransformer instance for testing."""
        return DataTransformer(config)
    
    @pytest.fixture
    def sample_user_data(self):
        """Sample user data for testing."""
        return {
            "id": 1,
            "username": "test_user",
            "key": "abcdef1234567890abcdef1234567890",
            "data_limit": 1073741824,
            "expire": "2024-12-31T23:59:59",
            "status": "active"
        }
    
    @pytest.fixture
    def sample_admin_data(self):
        """Sample admin data for testing."""
        return {
            "id": 1,
            "username": "test_admin",
            "is_sudo": True,
            "telegram_id": "123456789"
        }
    
    async def test_fault_tolerance_initialization(self, transformer):
        """Test fault tolerance system initialization."""
        assert transformer.fault_tolerance is not None
        assert hasattr(transformer, '_initialize_fault_tolerance')
        
        # Check that fallback values are configured
        fallback_config = transformer.fault_tolerance.fallback_strategy.config
        assert fallback_config.enable_cache_fallback is True
        assert fallback_config.enable_default_values is True
        assert 'user_status' in fallback_config.default_values
    
    async def test_transform_user_with_fault_tolerance_success(self, transformer, sample_user_data):
        """Test successful user transformation with fault tolerance."""
        result = await transformer.transform_user_with_fault_tolerance(sample_user_data)
        
        assert result is not None
        user_data, proxy_data = result
        assert user_data is not None
        assert isinstance(proxy_data, list)
        assert user_data["username"] == "test_user"
    
    async def test_transform_admin_with_fault_tolerance_success(self, transformer, sample_admin_data):
        """Test successful admin transformation with fault tolerance."""
        result = await transformer.transform_admin_with_fault_tolerance(sample_admin_data)
        
        assert result is not None
        assert result["username"] == "test_admin"
        assert result["is_sudo"] is True
    
    async def test_orchestrate_transformation_with_fault_tolerance(self, transformer):
        """Test orchestrated transformation with fault tolerance."""
        sample_data = {
            "users": [
                {
                    "id": 1,
                    "username": "user1",
                    "key": "abcdef1234567890abcdef1234567890",
                    "data_limit": 1073741824,
                    "expire": "2024-12-31T23:59:59",
                    "status": "active"
                },
                {
                    "id": 2,
                    "username": "user2",
                    "key": "fedcba0987654321fedcba0987654321",
                    "data_limit": 2147483648,
                    "expire": "2024-12-31T23:59:59",
                    "status": "active"
                }
            ],
            "admins": [
                {
                    "id": 1,
                    "username": "admin1",
                    "is_sudo": True,
                    "telegram_id": "123456789"
                }
            ]
        }
        
        result = await transformer.orchestrate_transformation_with_fault_tolerance(sample_data)
        
        assert "users" in result
        assert "proxies" in result
        assert "admins" in result
        assert len(result["users"]) == 2
        assert len(result["admins"]) == 1
        assert len(result["proxies"]) >= 2  # At least one proxy per user
    
    async def test_fault_tolerance_with_failing_transformation(self, transformer):
        """Test fault tolerance with failing transformation."""
        # Mock the transform method to fail initially then succeed
        original_transform = transformer.transform_user_for_marzban
        call_count = 0
        
        def mock_transform(user_data):
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise Exception("Simulated transformation failure")
            return original_transform(user_data)
        
        transformer.transform_user_for_marzban = mock_transform
        
        sample_user = {
            "id": 1,
            "username": "test_user",
            "key": "abcdef1234567890abcdef1234567890",
            "data_limit": 1073741824,
            "expire": "2024-12-31T23:59:59",
            "status": "active"
        }
        
        # Should succeed after retry
        result = await transformer.transform_user_with_fault_tolerance(sample_user)
        assert result is not None
        assert call_count == 2  # Failed once, succeeded on retry
    
    async def test_fault_tolerance_status_reporting(self, transformer):
        """Test fault tolerance status reporting."""
        status = transformer.get_fault_tolerance_status()
        
        assert "circuit_breakers" in status
        assert "services" in status
        assert "timestamp" in status
        
        # Check that migration circuit breaker exists
        circuit_status = status["circuit_breakers"]
        assert "migration" in circuit_status
    
    async def test_fault_tolerance_reset(self, transformer):
        """Test fault tolerance system reset."""
        # This should not raise any exceptions
        transformer.reset_fault_tolerance()
        
        # Verify circuit breakers are reset
        status = transformer.get_fault_tolerance_status()
        migration_status = status["circuit_breakers"]["migration"]
        assert migration_status["state"] == "closed"
    
    async def test_health_check_registration(self, transformer):
        """Test health check registration."""
        # Health checks should be registered during initialization
        health_status = transformer.fault_tolerance.health_monitor.get_all_status()
        
        # Should have database and api health checks
        assert "database" in health_status or "api" in health_status
    
    async def test_fallback_cache_functionality(self, transformer):
        """Test fallback cache functionality."""
        cache = transformer.fault_tolerance.fallback_strategy.cache
        
        # Test cache operations
        cache.set("test_key", "test_value")
        cached_value = cache.get("test_key")
        assert cached_value == "test_value"
        
        # Test cache size
        assert cache.size() > 0
        
        # Test cache clear
        cache.clear()
        assert cache.size() == 0
    
    async def test_retry_configuration_customization(self, transformer):
        """Test custom retry configuration."""
        custom_retry_config = RetryConfig(
            max_attempts=5,
            base_delay=0.5,
            exponential_base=1.5
        )
        
        # Mock a function that fails a few times
        call_count = 0
        
        async def flaky_function():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception("Flaky failure")
            return "success"
        
        result = await transformer.fault_tolerance.execute_with_fault_tolerance(
            flaky_function,
            "test_circuit",
            "test_fallback",
            custom_retry_config
        )
        
        assert result == "success"
        assert call_count == 3


async def run_fault_tolerant_transformer_tests():
    """Run all fault tolerant transformer tests."""
    print("🧪 Running Fault Tolerant Transformer Tests")
    print("=" * 50)
    
    # Initialize transformer with fault tolerance
    config = AppConfig()
    transformer = DataTransformer(config)
    
    # Test initialization
    assert transformer.fault_tolerance is not None
    print("✅ Fault tolerance initialization test passed")
    
    # Test user transformation with fault tolerance
    sample_user = {
        "id": 1,
        "username": "test_user",
        "key": "abcdef1234567890abcdef1234567890",
        "data_limit": 1073741824,
        "expire": "2024-12-31T23:59:59",
        "status": "active"
    }
    
    result = await transformer.transform_user_with_fault_tolerance(sample_user)
    assert result is not None
    print("✅ User transformation with fault tolerance test passed")
    
    # Test admin transformation with fault tolerance
    sample_admin = {
        "id": 1,
        "username": "test_admin",
        "is_sudo": True,
        "telegram_id": "123456789"
    }
    
    result = await transformer.transform_admin_with_fault_tolerance(sample_admin)
    assert result is not None
    print("✅ Admin transformation with fault tolerance test passed")
    
    # Test orchestrated transformation
    sample_data = {
        "users": [sample_user],
        "admins": [sample_admin]
    }
    
    result = await transformer.orchestrate_transformation_with_fault_tolerance(sample_data)
    assert len(result["users"]) == 1
    assert len(result["admins"]) == 1
    print("✅ Orchestrated transformation with fault tolerance test passed")
    
    # Test status reporting
    status = transformer.get_fault_tolerance_status()
    assert "circuit_breakers" in status
    print("✅ Status reporting test passed")
    
    # Test reset functionality
    transformer.reset_fault_tolerance()
    print("✅ Reset functionality test passed")
    
    # Test fallback cache
    cache = transformer.fault_tolerance.fallback_strategy.cache
    cache.set("test", "value")
    assert cache.get("test") == "value"
    print("✅ Fallback cache test passed")
    
    print("\n📊 All Fault Tolerant Transformer Tests Passed! 🎉")
    return True


if __name__ == "__main__":
    success = asyncio.run(run_fault_tolerant_transformer_tests())
    exit(0 if success else 1)