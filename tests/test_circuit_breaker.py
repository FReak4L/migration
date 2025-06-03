"""
Tests for Circuit Breaker implementation.
"""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock
from datetime import datetime

from core.circuit_breaker import (
    CircuitBreaker, CircuitBreakerConfig, CircuitState,
    CircuitBreakerError, CircuitBreakerManager, circuit_breaker
)


class TestCircuitBreaker:
    """Test cases for CircuitBreaker class."""
    
    @pytest.fixture
    def config(self):
        """Circuit breaker configuration for testing."""
        return CircuitBreakerConfig(
            failure_threshold=2,
            recovery_timeout=1,  # 1 second for fast testing
            success_threshold=2,
            timeout=5.0
        )
    
    @pytest.fixture
    def breaker(self, config):
        """Circuit breaker instance for testing."""
        return CircuitBreaker("test_breaker", config)
    
    async def test_circuit_breaker_closed_state_success(self, breaker):
        """Test circuit breaker in closed state with successful calls."""
        async def successful_func():
            return "success"
        
        result = await breaker.call(successful_func)
        assert result == "success"
        assert breaker.get_state() == CircuitState.CLOSED
        assert breaker.get_metrics().successful_requests == 1
        assert breaker.get_metrics().failed_requests == 0
    
    async def test_circuit_breaker_opens_on_failures(self, breaker):
        """Test circuit breaker opens after failure threshold."""
        async def failing_func():
            raise Exception("Test failure")
        
        # First failure
        with pytest.raises(Exception):
            await breaker.call(failing_func)
        assert breaker.get_state() == CircuitState.CLOSED
        
        # Second failure - should open circuit
        with pytest.raises(Exception):
            await breaker.call(failing_func)
        assert breaker.get_state() == CircuitState.OPEN
        assert breaker.get_metrics().circuit_open_count == 1
    
    async def test_circuit_breaker_fails_fast_when_open(self, breaker):
        """Test circuit breaker fails fast when open."""
        async def failing_func():
            raise Exception("Test failure")
        
        # Trigger failures to open circuit
        for _ in range(2):
            with pytest.raises(Exception):
                await breaker.call(failing_func)
        
        assert breaker.get_state() == CircuitState.OPEN
        
        # Should fail fast without calling function
        with pytest.raises(CircuitBreakerError):
            await breaker.call(failing_func)
    
    async def test_circuit_breaker_half_open_recovery(self, breaker):
        """Test circuit breaker recovery through half-open state."""
        async def failing_func():
            raise Exception("Test failure")
        
        async def successful_func():
            return "success"
        
        # Open the circuit
        for _ in range(2):
            with pytest.raises(Exception):
                await breaker.call(failing_func)
        
        assert breaker.get_state() == CircuitState.OPEN
        
        # Wait for recovery timeout
        await asyncio.sleep(1.1)
        
        # First call should move to half-open
        result = await breaker.call(successful_func)
        assert result == "success"
        
        # Second successful call should close circuit
        result = await breaker.call(successful_func)
        assert result == "success"
        assert breaker.get_state() == CircuitState.CLOSED
    
    async def test_circuit_breaker_timeout(self, breaker):
        """Test circuit breaker timeout functionality."""
        async def slow_func():
            await asyncio.sleep(10)  # Longer than timeout
            return "success"
        
        with pytest.raises(CircuitBreakerError):
            await breaker.call(slow_func)
        
        assert breaker.get_metrics().failed_requests == 1
    
    async def test_sync_function_execution(self, breaker):
        """Test circuit breaker with synchronous functions."""
        def sync_func():
            return "sync_success"
        
        result = await breaker.call(sync_func)
        assert result == "sync_success"
        assert breaker.get_metrics().successful_requests == 1


class TestCircuitBreakerManager:
    """Test cases for CircuitBreakerManager class."""
    
    def test_get_breaker_creates_new(self):
        """Test manager creates new circuit breaker."""
        manager = CircuitBreakerManager()
        breaker = manager.get_breaker("test")
        
        assert breaker.name == "test"
        assert "test" in manager._breakers
    
    def test_get_breaker_returns_existing(self):
        """Test manager returns existing circuit breaker."""
        manager = CircuitBreakerManager()
        breaker1 = manager.get_breaker("test")
        breaker2 = manager.get_breaker("test")
        
        assert breaker1 is breaker2
    
    def test_get_all_metrics(self):
        """Test getting metrics for all circuit breakers."""
        manager = CircuitBreakerManager()
        breaker1 = manager.get_breaker("test1")
        breaker2 = manager.get_breaker("test2")
        
        metrics = manager.get_all_metrics()
        assert "test1" in metrics
        assert "test2" in metrics
    
    def test_get_health_status(self):
        """Test getting health status for all circuit breakers."""
        manager = CircuitBreakerManager()
        breaker = manager.get_breaker("test")
        
        status = manager.get_health_status()
        assert "test" in status
        assert status["test"]["state"] == "closed"
        assert status["test"]["health"] == "healthy"


class TestCircuitBreakerDecorator:
    """Test cases for circuit breaker decorator."""
    
    async def test_decorator_success(self):
        """Test circuit breaker decorator with successful function."""
        @circuit_breaker("test_decorator")
        async def decorated_func():
            return "decorated_success"
        
        result = await decorated_func()
        assert result == "decorated_success"
        
        # Check that circuit breaker was created
        assert hasattr(decorated_func, '_circuit_breaker')
    
    async def test_decorator_failure(self):
        """Test circuit breaker decorator with failing function."""
        @circuit_breaker("test_decorator_fail", CircuitBreakerConfig(failure_threshold=1))
        async def decorated_func():
            raise Exception("Decorated failure")
        
        # First call should fail and open circuit
        with pytest.raises(Exception):
            await decorated_func()
        
        # Second call should fail fast
        with pytest.raises(CircuitBreakerError):
            await decorated_func()


async def run_circuit_breaker_tests():
    """Run all circuit breaker tests."""
    print("🧪 Running Circuit Breaker Tests")
    print("=" * 50)
    
    # Test basic functionality
    config = CircuitBreakerConfig(failure_threshold=2, recovery_timeout=1)
    breaker = CircuitBreaker("test", config)
    
    # Test successful call
    async def success_func():
        return "success"
    
    result = await breaker.call(success_func)
    assert result == "success"
    print("✅ Successful call test passed")
    
    # Test failure and circuit opening
    async def fail_func():
        raise Exception("Test failure")
    
    try:
        await breaker.call(fail_func)
    except Exception:
        pass
    
    try:
        await breaker.call(fail_func)
    except Exception:
        pass
    
    assert breaker.get_state() == CircuitState.OPEN
    print("✅ Circuit opening test passed")
    
    # Test fail-fast behavior
    try:
        await breaker.call(fail_func)
        assert False, "Should have failed fast"
    except CircuitBreakerError:
        print("✅ Fail-fast test passed")
    
    # Test recovery
    await asyncio.sleep(1.1)  # Wait for recovery timeout
    
    # Should move to half-open and then closed
    await breaker.call(success_func)
    await breaker.call(success_func)
    
    assert breaker.get_state() == CircuitState.CLOSED
    print("✅ Recovery test passed")
    
    # Test manager
    manager = CircuitBreakerManager()
    breaker1 = manager.get_breaker("test1")
    breaker2 = manager.get_breaker("test2")
    
    assert len(manager._breakers) == 2
    print("✅ Manager test passed")
    
    # Test decorator
    @circuit_breaker("decorator_test")
    async def decorated_success():
        return "decorated"
    
    result = await decorated_success()
    assert result == "decorated"
    print("✅ Decorator test passed")
    
    print("\n📊 All Circuit Breaker Tests Passed! 🎉")
    return True


if __name__ == "__main__":
    success = asyncio.run(run_circuit_breaker_tests())
    exit(0 if success else 1)