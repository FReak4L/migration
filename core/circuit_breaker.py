"""
Circuit Breaker Pattern Implementation for Fault Tolerance

This module implements the Circuit Breaker pattern to provide fault tolerance
for external API calls and database operations during migration.
"""

import asyncio
import time
from enum import Enum
from typing import Any, Callable, Dict, Optional, Union, List
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


class CircuitState(Enum):
    """Circuit breaker states."""
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Circuit is open, calls fail fast
    HALF_OPEN = "half_open"  # Testing if service is back


@dataclass
class CircuitBreakerConfig:
    """Configuration for circuit breaker."""
    failure_threshold: int = 5  # Number of failures before opening
    recovery_timeout: int = 60  # Seconds before trying half-open
    success_threshold: int = 3  # Successes needed to close from half-open
    timeout: float = 30.0  # Request timeout in seconds
    expected_exception: tuple = (Exception,)  # Exceptions that count as failures


@dataclass
class CircuitBreakerMetrics:
    """Metrics for circuit breaker monitoring."""
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    circuit_open_count: int = 0
    last_failure_time: Optional[datetime] = None
    last_success_time: Optional[datetime] = None
    consecutive_failures: int = 0
    consecutive_successes: int = 0


class CircuitBreakerError(Exception):
    """Exception raised when circuit breaker is open."""
    pass


class CircuitBreaker:
    """
    Circuit Breaker implementation for fault tolerance.
    
    Provides automatic failure detection and recovery for external services.
    """
    
    def __init__(self, name: str, config: CircuitBreakerConfig = None):
        self.name = name
        self.config = config or CircuitBreakerConfig()
        self.state = CircuitState.CLOSED
        self.metrics = CircuitBreakerMetrics()
        self._lock = asyncio.Lock()
        self._last_failure_time = 0
        
        logger.info(f"Circuit breaker '{name}' initialized with config: {self.config}")
    
    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute a function with circuit breaker protection.
        
        Args:
            func: Function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments
            
        Returns:
            Function result
            
        Raises:
            CircuitBreakerError: When circuit is open
            Exception: Original function exceptions
        """
        async with self._lock:
            self.metrics.total_requests += 1
            
            # Check if circuit should be opened
            if self.state == CircuitState.OPEN:
                if self._should_attempt_reset():
                    self.state = CircuitState.HALF_OPEN
                    logger.info(f"Circuit breaker '{self.name}' moved to HALF_OPEN state")
                else:
                    self.metrics.failed_requests += 1
                    raise CircuitBreakerError(
                        f"Circuit breaker '{self.name}' is OPEN. "
                        f"Last failure: {self.metrics.last_failure_time}"
                    )
        
        # Execute the function
        try:
            # Apply timeout if specified
            if self.config.timeout > 0:
                result = await asyncio.wait_for(
                    self._execute_async(func, *args, **kwargs),
                    timeout=self.config.timeout
                )
            else:
                result = await self._execute_async(func, *args, **kwargs)
            
            # Handle success
            await self._on_success()
            return result
            
        except self.config.expected_exception as e:
            # Handle expected failures
            await self._on_failure(e)
            raise
        except asyncio.TimeoutError as e:
            # Handle timeout as failure
            await self._on_failure(e)
            raise CircuitBreakerError(f"Request timeout after {self.config.timeout}s") from e
    
    async def _execute_async(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function, handling both sync and async functions."""
        if asyncio.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        else:
            # Run sync function in thread pool
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, lambda: func(*args, **kwargs))
    
    async def _on_success(self):
        """Handle successful function execution."""
        async with self._lock:
            self.metrics.successful_requests += 1
            self.metrics.consecutive_successes += 1
            self.metrics.consecutive_failures = 0
            self.metrics.last_success_time = datetime.utcnow()
            
            # Close circuit if in half-open state and enough successes
            if (self.state == CircuitState.HALF_OPEN and 
                self.metrics.consecutive_successes >= self.config.success_threshold):
                self.state = CircuitState.CLOSED
                logger.info(f"Circuit breaker '{self.name}' moved to CLOSED state")
    
    async def _on_failure(self, exception: Exception):
        """Handle failed function execution."""
        async with self._lock:
            self.metrics.failed_requests += 1
            self.metrics.consecutive_failures += 1
            self.metrics.consecutive_successes = 0
            self.metrics.last_failure_time = datetime.utcnow()
            self._last_failure_time = time.time()
            
            logger.warning(f"Circuit breaker '{self.name}' recorded failure: {exception}")
            
            # Open circuit if failure threshold reached
            if (self.state == CircuitState.CLOSED and 
                self.metrics.consecutive_failures >= self.config.failure_threshold):
                self.state = CircuitState.OPEN
                self.metrics.circuit_open_count += 1
                logger.error(f"Circuit breaker '{self.name}' moved to OPEN state after {self.metrics.consecutive_failures} failures")
            
            # Return to open if half-open fails
            elif self.state == CircuitState.HALF_OPEN:
                self.state = CircuitState.OPEN
                self.metrics.circuit_open_count += 1
                logger.warning(f"Circuit breaker '{self.name}' returned to OPEN state from HALF_OPEN")
    
    def _should_attempt_reset(self) -> bool:
        """Check if circuit should attempt reset to half-open."""
        return (time.time() - self._last_failure_time) >= self.config.recovery_timeout
    
    def get_state(self) -> CircuitState:
        """Get current circuit state."""
        return self.state
    
    def get_metrics(self) -> CircuitBreakerMetrics:
        """Get circuit breaker metrics."""
        return self.metrics
    
    def reset(self):
        """Manually reset circuit breaker to closed state."""
        with asyncio.Lock():
            self.state = CircuitState.CLOSED
            self.metrics.consecutive_failures = 0
            self.metrics.consecutive_successes = 0
            logger.info(f"Circuit breaker '{self.name}' manually reset to CLOSED state")


class CircuitBreakerManager:
    """
    Manager for multiple circuit breakers.
    
    Provides centralized management and monitoring of circuit breakers.
    """
    
    def __init__(self):
        self._breakers: Dict[str, CircuitBreaker] = {}
        self._default_config = CircuitBreakerConfig()
    
    def get_breaker(self, name: str, config: CircuitBreakerConfig = None) -> CircuitBreaker:
        """
        Get or create a circuit breaker.
        
        Args:
            name: Circuit breaker name
            config: Optional configuration
            
        Returns:
            CircuitBreaker instance
        """
        if name not in self._breakers:
            self._breakers[name] = CircuitBreaker(name, config or self._default_config)
        return self._breakers[name]
    
    def get_all_metrics(self) -> Dict[str, CircuitBreakerMetrics]:
        """Get metrics for all circuit breakers."""
        return {name: breaker.get_metrics() for name, breaker in self._breakers.items()}
    
    def get_health_status(self) -> Dict[str, Dict[str, Any]]:
        """Get health status of all circuit breakers."""
        status = {}
        for name, breaker in self._breakers.items():
            metrics = breaker.get_metrics()
            status[name] = {
                "state": breaker.get_state().value,
                "health": "healthy" if breaker.get_state() == CircuitState.CLOSED else "degraded",
                "total_requests": metrics.total_requests,
                "success_rate": (
                    metrics.successful_requests / metrics.total_requests * 100
                    if metrics.total_requests > 0 else 0
                ),
                "consecutive_failures": metrics.consecutive_failures,
                "last_failure": metrics.last_failure_time.isoformat() if metrics.last_failure_time else None
            }
        return status
    
    def reset_all(self):
        """Reset all circuit breakers."""
        for breaker in self._breakers.values():
            breaker.reset()
        logger.info("All circuit breakers reset")


# Global circuit breaker manager instance
circuit_manager = CircuitBreakerManager()


def circuit_breaker(name: str, config: CircuitBreakerConfig = None):
    """
    Decorator for applying circuit breaker to functions.
    
    Args:
        name: Circuit breaker name
        config: Optional configuration
        
    Returns:
        Decorated function
    """
    def decorator(func):
        breaker = circuit_manager.get_breaker(name, config)
        
        async def wrapper(*args, **kwargs):
            return await breaker.call(func, *args, **kwargs)
        
        # Preserve function metadata
        wrapper.__name__ = func.__name__
        wrapper.__doc__ = func.__doc__
        wrapper._circuit_breaker = breaker
        
        return wrapper
    return decorator


# Predefined configurations for common use cases
API_CIRCUIT_CONFIG = CircuitBreakerConfig(
    failure_threshold=3,
    recovery_timeout=30,
    success_threshold=2,
    timeout=15.0,
    expected_exception=(ConnectionError, TimeoutError, Exception)
)

DATABASE_CIRCUIT_CONFIG = CircuitBreakerConfig(
    failure_threshold=5,
    recovery_timeout=60,
    success_threshold=3,
    timeout=30.0,
    expected_exception=(ConnectionError, TimeoutError, Exception)
)

MIGRATION_CIRCUIT_CONFIG = CircuitBreakerConfig(
    failure_threshold=2,
    recovery_timeout=45,
    success_threshold=1,
    timeout=60.0,
    expected_exception=(Exception,)
)