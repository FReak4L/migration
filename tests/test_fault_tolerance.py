"""
Tests for Fault Tolerance system.
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime

from core.fault_tolerance import (
    RetryMechanism, RetryConfig, FallbackStrategy, FallbackConfig,
    HealthMonitor, HealthCheckConfig, FaultToleranceManager,
    RetryExhaustedError, FallbackError, fault_tolerant
)


class TestRetryMechanism:
    """Test cases for RetryMechanism class."""
    
    def test_retry_config_defaults(self):
        """Test retry configuration defaults."""
        config = RetryConfig()
        assert config.max_attempts == 3
        assert config.base_delay == 1.0
        assert config.exponential_base == 2.0
        assert config.jitter is True
    
    async def test_successful_execution_no_retry(self):
        """Test successful execution without retries."""
        retry = RetryMechanism()
        
        async def success_func():
            return "success"
        
        result = await retry.execute(success_func)
        assert result == "success"
    
    async def test_retry_on_failure(self):
        """Test retry mechanism on failures."""
        retry = RetryMechanism(RetryConfig(max_attempts=3, base_delay=0.1))
        call_count = 0
        
        async def failing_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise Exception(f"Failure {call_count}")
            return "success_after_retries"
        
        result = await retry.execute(failing_func)
        assert result == "success_after_retries"
        assert call_count == 3
    
    async def test_retry_exhausted(self):
        """Test retry exhaustion."""
        retry = RetryMechanism(RetryConfig(max_attempts=2, base_delay=0.1))
        
        async def always_failing_func():
            raise Exception("Always fails")
        
        with pytest.raises(RetryExhaustedError):
            await retry.execute(always_failing_func)
    
    async def test_sync_function_retry(self):
        """Test retry mechanism with sync functions."""
        retry = RetryMechanism(RetryConfig(max_attempts=2, base_delay=0.1))
        call_count = 0
        
        def sync_func():
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise Exception("First failure")
            return "sync_success"
        
        result = await retry.execute(sync_func)
        assert result == "sync_success"
        assert call_count == 2


class TestFallbackStrategy:
    """Test cases for FallbackStrategy class."""
    
    def test_fallback_config_defaults(self):
        """Test fallback configuration defaults."""
        config = FallbackConfig()
        assert config.enable_cache_fallback is True
        assert config.cache_ttl == 300
        assert config.enable_default_values is True
    
    async def test_successful_execution_with_caching(self):
        """Test successful execution with result caching."""
        fallback = FallbackStrategy()
        
        async def success_func():
            return "cached_result"
        
        result = await fallback.execute_with_fallback(success_func, "test_key")
        assert result == "cached_result"
        
        # Check if result was cached
        cached = fallback.cache.get("test_key")
        assert cached == "cached_result"
    
    async def test_cache_fallback(self):
        """Test fallback to cached result."""
        fallback = FallbackStrategy()
        
        # Pre-populate cache
        fallback.cache.set("test_key", "cached_value")
        
        async def failing_func():
            raise Exception("Function failed")
        
        result = await fallback.execute_with_fallback(failing_func, "test_key")
        assert result == "cached_value"
    
    async def test_default_value_fallback(self):
        """Test fallback to default values."""
        config = FallbackConfig(
            enable_cache_fallback=False,
            default_values={"test_key": "default_value"}
        )
        fallback = FallbackStrategy(config)
        
        async def failing_func():
            raise Exception("Function failed")
        
        result = await fallback.execute_with_fallback(failing_func, "test_key")
        assert result == "default_value"
    
    async def test_fallback_failure(self):
        """Test when all fallback strategies fail."""
        config = FallbackConfig(
            enable_cache_fallback=False,
            enable_default_values=False
        )
        fallback = FallbackStrategy(config)
        
        async def failing_func():
            raise Exception("Function failed")
        
        with pytest.raises(FallbackError):
            await fallback.execute_with_fallback(failing_func, "test_key")


class TestHealthMonitor:
    """Test cases for HealthMonitor class."""
    
    def test_health_config_defaults(self):
        """Test health check configuration defaults."""
        config = HealthCheckConfig()
        assert config.check_interval == 30
        assert config.timeout == 10.0
        assert config.failure_threshold == 3
    
    async def test_service_registration(self):
        """Test service registration for health monitoring."""
        monitor = HealthMonitor(HealthCheckConfig(check_interval=0.1))
        
        async def health_check():
            return True
        
        monitor.register_service("test_service", health_check)
        
        # Wait a bit for health check to run
        await asyncio.sleep(0.2)
        
        status = monitor.get_service_status("test_service")
        assert status is not None
        assert status["status"] == "healthy"
        
        monitor.stop_monitoring("test_service")
    
    async def test_service_health_failure(self):
        """Test service health failure detection."""
        monitor = HealthMonitor(HealthCheckConfig(
            check_interval=0.1,
            failure_threshold=2
        ))
        
        async def failing_health_check():
            raise Exception("Health check failed")
        
        monitor.register_service("failing_service", failing_health_check)
        
        # Wait for failures to accumulate
        await asyncio.sleep(0.3)
        
        status = monitor.get_service_status("failing_service")
        assert status["consecutive_failures"] >= 2
        
        monitor.stop_monitoring("failing_service")
    
    def test_is_healthy_check(self):
        """Test health status checking."""
        monitor = HealthMonitor()
        
        # Mock health status
        monitor._health_status["healthy_service"] = {"status": "healthy"}
        monitor._health_status["unhealthy_service"] = {"status": "unhealthy"}
        
        assert monitor.is_healthy("healthy_service") is True
        assert monitor.is_healthy("unhealthy_service") is False
        assert monitor.is_healthy("nonexistent_service") is False


class TestFaultToleranceManager:
    """Test cases for FaultToleranceManager class."""
    
    async def test_comprehensive_fault_tolerance(self):
        """Test comprehensive fault tolerance execution."""
        manager = FaultToleranceManager()
        call_count = 0
        
        async def flaky_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise Exception("Flaky failure")
            return "success_after_retry"
        
        result = await manager.execute_with_fault_tolerance(
            flaky_func,
            "test_circuit",
            "test_fallback"
        )
        
        assert result == "success_after_retry"
        assert call_count == 2
    
    async def test_fallback_on_circuit_open(self):
        """Test fallback when circuit breaker is open."""
        manager = FaultToleranceManager()
        
        # Pre-populate fallback cache
        manager.fallback_strategy.cache.set("fallback_key", "fallback_value")
        
        async def always_failing_func():
            raise Exception("Always fails")
        
        # First, open the circuit by causing failures
        circuit = manager.circuit_manager.get_breaker("test_circuit")
        for _ in range(5):  # Exceed failure threshold
            try:
                await circuit.call(always_failing_func)
            except:
                pass
        
        # Now test fallback
        result = await manager.execute_with_fault_tolerance(
            always_failing_func,
            "test_circuit",
            "fallback_key"
        )
        
        assert result == "fallback_value"
    
    def test_system_health_status(self):
        """Test system health status reporting."""
        manager = FaultToleranceManager()
        
        health_status = manager.get_system_health()
        
        assert "circuit_breakers" in health_status
        assert "services" in health_status
        assert "timestamp" in health_status
    
    def test_reset_all_circuits(self):
        """Test resetting all circuit breakers."""
        manager = FaultToleranceManager()
        
        # Get a circuit breaker and simulate failure
        circuit = manager.circuit_manager.get_breaker("test_reset")
        circuit.metrics.consecutive_failures = 5
        
        # Reset all circuits
        manager.reset_all_circuits()
        
        # Check that circuit was reset
        assert circuit.metrics.consecutive_failures == 0


class TestFaultTolerantDecorator:
    """Test cases for fault_tolerant decorator."""
    
    async def test_decorator_success(self):
        """Test fault tolerant decorator with successful function."""
        @fault_tolerant("decorator_circuit")
        async def decorated_func():
            return "decorated_success"
        
        result = await decorated_func()
        assert result == "decorated_success"
    
    async def test_decorator_with_retry(self):
        """Test fault tolerant decorator with retry."""
        call_count = 0
        
        @fault_tolerant("decorator_circuit", retry_config=RetryConfig(max_attempts=3, base_delay=0.1))
        async def flaky_decorated_func():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise Exception("Flaky failure")
            return "success_after_retry"
        
        result = await flaky_decorated_func()
        assert result == "success_after_retry"
        assert call_count == 2


async def run_fault_tolerance_tests():
    """Run all fault tolerance tests."""
    print("🧪 Running Fault Tolerance Tests")
    print("=" * 50)
    
    # Test retry mechanism
    retry = RetryMechanism(RetryConfig(max_attempts=3, base_delay=0.1))
    
    async def success_after_retry():
        global retry_count
        retry_count = getattr(success_after_retry, 'count', 0) + 1
        setattr(success_after_retry, 'count', retry_count)
        if retry_count < 2:
            raise Exception("Retry test")
        return "retry_success"
    
    result = await retry.execute(success_after_retry)
    assert result == "retry_success"
    print("✅ Retry mechanism test passed")
    
    # Test fallback strategy
    fallback = FallbackStrategy(FallbackConfig(
        default_values={"test": "fallback_value"}
    ))
    
    async def failing_func():
        raise Exception("Fallback test")
    
    result = await fallback.execute_with_fallback(failing_func, "test")
    assert result == "fallback_value"
    print("✅ Fallback strategy test passed")
    
    # Test health monitor
    monitor = HealthMonitor(HealthCheckConfig(check_interval=0.1))
    
    async def health_check():
        return True
    
    monitor.register_service("test_service", health_check)
    await asyncio.sleep(0.2)
    
    assert monitor.is_healthy("test_service")
    monitor.stop_monitoring("test_service")
    print("✅ Health monitor test passed")
    
    # Test comprehensive fault tolerance
    manager = FaultToleranceManager()
    
    async def comprehensive_test():
        return "comprehensive_success"
    
    result = await manager.execute_with_fault_tolerance(
        comprehensive_test,
        "test_circuit",
        "test_fallback"
    )
    assert result == "comprehensive_success"
    print("✅ Comprehensive fault tolerance test passed")
    
    # Test decorator
    @fault_tolerant("decorator_test")
    async def decorated_test():
        return "decorator_success"
    
    result = await decorated_test()
    assert result == "decorator_success"
    print("✅ Fault tolerant decorator test passed")
    
    print("\n📊 All Fault Tolerance Tests Passed! 🎉")
    return True


if __name__ == "__main__":
    success = asyncio.run(run_fault_tolerance_tests())
    exit(0 if success else 1)