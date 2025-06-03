#!/usr/bin/env python3
"""
Simple test for fault tolerance functionality.
"""

import asyncio
import sys
import os

# Add the project root to Python path
sys.path.insert(0, '/workspace/migration')

# Set environment variables
os.environ.update({
    'MARZNESHIN_API_URL': 'http://test-marzneshin.com',
    'MARZNESHIN_USERNAME': 'test_user',
    'MARZNESHIN_PASSWORD': 'test_pass',
    'MARZBAN_API_URL': 'http://test-marzban.com',
    'MARZBAN_USERNAME': 'test_user',
    'MARZBAN_PASSWORD': 'test_pass'
})

from core.fault_tolerance import FaultToleranceManager, RetryConfig
from core.circuit_breaker import CircuitBreaker, CircuitState

async def test_fault_tolerance():
    """Test basic fault tolerance functionality."""
    print("🧪 Testing Fault Tolerance System")
    print("=" * 40)
    
    # Test 1: Basic fault tolerance manager
    manager = FaultToleranceManager()
    print("✅ FaultToleranceManager initialized")
    
    # Test 2: Circuit breaker functionality
    async def success_func():
        return "success"
    
    async def failure_func():
        raise Exception("Test failure")
    
    result = await manager.execute_with_fault_tolerance(
        success_func,
        "test_circuit"
    )
    assert result == "success"
    print("✅ Successful execution test passed")
    
    # Test 3: Retry mechanism
    attempt_count = 0
    async def retry_func():
        nonlocal attempt_count
        attempt_count += 1
        if attempt_count < 3:
            raise Exception("Retry test")
        return "retry_success"
    
    result = await manager.execute_with_fault_tolerance(
        retry_func,
        "retry_circuit"
    )
    assert result == "retry_success"
    assert attempt_count == 3
    print("✅ Retry mechanism test passed")
    
    # Test 4: Health monitoring
    health = manager.get_system_health()
    assert 'circuit_breakers' in health
    assert 'services' in health
    assert 'timestamp' in health
    print("✅ Health monitoring test passed")
    
    print("\n📊 All Fault Tolerance Tests Passed! 🎉")
    return True

if __name__ == "__main__":
    success = asyncio.run(test_fault_tolerance())
    sys.exit(0 if success else 1)