"""
Fault Tolerance System for Migration Operations

This module provides comprehensive fault tolerance capabilities including
circuit breakers, retry mechanisms, fallback strategies, and health monitoring.
"""

import asyncio
import time
from typing import Any, Callable, Dict, List, Optional, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
import logging
import json
from pathlib import Path

from .circuit_breaker import (
    CircuitBreaker, CircuitBreakerConfig, CircuitBreakerManager,
    API_CIRCUIT_CONFIG, DATABASE_CIRCUIT_CONFIG, MIGRATION_CIRCUIT_CONFIG,
    circuit_manager
)

logger = logging.getLogger(__name__)


@dataclass
class RetryConfig:
    """Configuration for retry mechanism."""
    max_attempts: int = 3
    base_delay: float = 1.0  # Base delay in seconds
    max_delay: float = 60.0  # Maximum delay in seconds
    exponential_base: float = 2.0  # Exponential backoff base
    jitter: bool = True  # Add random jitter to delays


@dataclass
class FallbackConfig:
    """Configuration for fallback strategies."""
    enable_cache_fallback: bool = True
    cache_ttl: int = 300  # Cache TTL in seconds
    enable_default_values: bool = True
    default_values: Dict[str, Any] = field(default_factory=dict)


@dataclass
class HealthCheckConfig:
    """Configuration for health checks."""
    check_interval: int = 30  # Health check interval in seconds
    timeout: float = 10.0  # Health check timeout
    failure_threshold: int = 3  # Failures before marking unhealthy


class FaultToleranceError(Exception):
    """Base exception for fault tolerance errors."""
    pass


class RetryExhaustedError(FaultToleranceError):
    """Exception raised when all retry attempts are exhausted."""
    pass


class FallbackError(FaultToleranceError):
    """Exception raised when fallback strategies fail."""
    pass


class RetryMechanism:
    """
    Retry mechanism with exponential backoff and jitter.
    """
    
    def __init__(self, config: RetryConfig = None):
        self.config = config or RetryConfig()
    
    async def execute(self, func: Callable, *args, **kwargs) -> Any:
        """
        Execute function with retry logic.
        
        Args:
            func: Function to execute
            *args: Function arguments
            **kwargs: Function keyword arguments
            
        Returns:
            Function result
            
        Raises:
            RetryExhaustedError: When all attempts fail
        """
        last_exception = None
        
        for attempt in range(self.config.max_attempts):
            try:
                if asyncio.iscoroutinefunction(func):
                    return await func(*args, **kwargs)
                else:
                    loop = asyncio.get_event_loop()
                    return await loop.run_in_executor(None, lambda: func(*args, **kwargs))
                    
            except Exception as e:
                last_exception = e
                
                if attempt < self.config.max_attempts - 1:
                    delay = self._calculate_delay(attempt)
                    logger.warning(
                        f"Attempt {attempt + 1} failed: {e}. "
                        f"Retrying in {delay:.2f}s..."
                    )
                    await asyncio.sleep(delay)
                else:
                    logger.error(f"All {self.config.max_attempts} attempts failed")
        
        raise RetryExhaustedError(
            f"Failed after {self.config.max_attempts} attempts. "
            f"Last error: {last_exception}"
        ) from last_exception
    
    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay for exponential backoff with jitter."""
        delay = min(
            self.config.base_delay * (self.config.exponential_base ** attempt),
            self.config.max_delay
        )
        
        if self.config.jitter:
            import random
            delay *= (0.5 + random.random() * 0.5)  # Add 0-50% jitter
        
        return delay


class FallbackCache:
    """
    Simple in-memory cache for fallback data.
    """
    
    def __init__(self, ttl: int = 300):
        self.ttl = ttl
        self._cache: Dict[str, Dict[str, Any]] = {}
    
    def get(self, key: str) -> Optional[Any]:
        """Get value from cache if not expired."""
        if key in self._cache:
            entry = self._cache[key]
            if time.time() - entry['timestamp'] < self.ttl:
                return entry['value']
            else:
                del self._cache[key]
        return None
    
    def set(self, key: str, value: Any):
        """Set value in cache with timestamp."""
        self._cache[key] = {
            'value': value,
            'timestamp': time.time()
        }
    
    def clear(self):
        """Clear all cache entries."""
        self._cache.clear()
    
    def size(self) -> int:
        """Get cache size."""
        return len(self._cache)


class FallbackStrategy:
    """
    Fallback strategy implementation.
    """
    
    def __init__(self, config: FallbackConfig = None):
        self.config = config or FallbackConfig()
        self.cache = FallbackCache(self.config.cache_ttl) if self.config.enable_cache_fallback else None
    
    async def execute_with_fallback(self, 
                                  primary_func: Callable,
                                  fallback_key: str,
                                  *args, **kwargs) -> Any:
        """
        Execute function with fallback strategies.
        
        Args:
            primary_func: Primary function to execute
            fallback_key: Key for cache/default value lookup
            *args: Function arguments
            **kwargs: Function keyword arguments
            
        Returns:
            Function result or fallback value
        """
        try:
            # Try primary function
            result = await self._execute_async(primary_func, *args, **kwargs)
            
            # Cache successful result
            if self.cache:
                self.cache.set(fallback_key, result)
            
            return result
            
        except Exception as e:
            logger.warning(f"Primary function failed: {e}. Trying fallback strategies...")
            
            # Try cache fallback
            if self.cache:
                cached_result = self.cache.get(fallback_key)
                if cached_result is not None:
                    logger.info(f"Using cached fallback for key: {fallback_key}")
                    return cached_result
            
            # Try default value fallback
            if self.config.enable_default_values and fallback_key in self.config.default_values:
                default_value = self.config.default_values[fallback_key]
                logger.info(f"Using default fallback for key: {fallback_key}")
                return default_value
            
            # All fallback strategies failed
            raise FallbackError(f"All fallback strategies failed for key: {fallback_key}") from e
    
    async def _execute_async(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function, handling both sync and async functions."""
        if asyncio.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        else:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, lambda: func(*args, **kwargs))


class HealthMonitor:
    """
    Health monitoring for external services.
    """
    
    def __init__(self, config: HealthCheckConfig = None):
        self.config = config or HealthCheckConfig()
        self._health_status: Dict[str, Dict[str, Any]] = {}
        self._monitoring_tasks: Dict[str, asyncio.Task] = {}
    
    def register_service(self, 
                        name: str, 
                        health_check_func: Callable,
                        *args, **kwargs):
        """
        Register a service for health monitoring.
        
        Args:
            name: Service name
            health_check_func: Function to check service health
            *args: Health check function arguments
            **kwargs: Health check function keyword arguments
        """
        self._health_status[name] = {
            'status': 'unknown',
            'last_check': None,
            'consecutive_failures': 0,
            'last_error': None
        }
        
        # Start monitoring task
        task = asyncio.create_task(
            self._monitor_service(name, health_check_func, *args, **kwargs)
        )
        self._monitoring_tasks[name] = task
        
        logger.info(f"Registered health monitoring for service: {name}")
    
    async def _monitor_service(self, 
                             name: str, 
                             health_check_func: Callable,
                             *args, **kwargs):
        """Monitor service health continuously."""
        while True:
            try:
                # Perform health check with timeout
                await asyncio.wait_for(
                    self._execute_health_check(health_check_func, *args, **kwargs),
                    timeout=self.config.timeout
                )
                
                # Health check passed
                self._health_status[name].update({
                    'status': 'healthy',
                    'last_check': datetime.utcnow(),
                    'consecutive_failures': 0,
                    'last_error': None
                })
                
            except Exception as e:
                # Health check failed
                self._health_status[name]['consecutive_failures'] += 1
                self._health_status[name]['last_error'] = str(e)
                self._health_status[name]['last_check'] = datetime.utcnow()
                
                if self._health_status[name]['consecutive_failures'] >= self.config.failure_threshold:
                    self._health_status[name]['status'] = 'unhealthy'
                    logger.error(f"Service {name} marked as unhealthy after {self.config.failure_threshold} failures")
                
            await asyncio.sleep(self.config.check_interval)
    
    async def _execute_health_check(self, func: Callable, *args, **kwargs):
        """Execute health check function."""
        if asyncio.iscoroutinefunction(func):
            return await func(*args, **kwargs)
        else:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(None, lambda: func(*args, **kwargs))
    
    def get_service_status(self, name: str) -> Optional[Dict[str, Any]]:
        """Get health status for a specific service."""
        return self._health_status.get(name)
    
    def get_all_status(self) -> Dict[str, Dict[str, Any]]:
        """Get health status for all services."""
        return self._health_status.copy()
    
    def is_healthy(self, name: str) -> bool:
        """Check if a service is healthy."""
        status = self._health_status.get(name, {})
        return status.get('status') == 'healthy'
    
    def stop_monitoring(self, name: str):
        """Stop monitoring a service."""
        if name in self._monitoring_tasks:
            self._monitoring_tasks[name].cancel()
            del self._monitoring_tasks[name]
            logger.info(f"Stopped monitoring service: {name}")
    
    def stop_all_monitoring(self):
        """Stop monitoring all services."""
        for task in self._monitoring_tasks.values():
            task.cancel()
        self._monitoring_tasks.clear()
        logger.info("Stopped all health monitoring")


class FaultToleranceManager:
    """
    Comprehensive fault tolerance manager.
    
    Integrates circuit breakers, retry mechanisms, fallback strategies,
    and health monitoring.
    """
    
    def __init__(self):
        self.circuit_manager = circuit_manager
        self.retry_mechanism = RetryMechanism()
        self.fallback_strategy = FallbackStrategy()
        self.health_monitor = HealthMonitor()
        
        # Initialize circuit breakers for common services
        self._initialize_circuit_breakers()
        
        logger.info("Fault tolerance manager initialized")
    
    def _initialize_circuit_breakers(self):
        """Initialize circuit breakers for common migration services."""
        # API circuit breaker
        self.circuit_manager.get_breaker("marzban_api", API_CIRCUIT_CONFIG)
        self.circuit_manager.get_breaker("marzneshin_api", API_CIRCUIT_CONFIG)
        
        # Database circuit breaker
        self.circuit_manager.get_breaker("database", DATABASE_CIRCUIT_CONFIG)
        
        # Migration circuit breaker
        self.circuit_manager.get_breaker("migration", MIGRATION_CIRCUIT_CONFIG)
    
    async def execute_with_fault_tolerance(self,
                                         func: Callable,
                                         circuit_name: str,
                                         fallback_key: str = None,
                                         retry_config: RetryConfig = None,
                                         *args, **kwargs) -> Any:
        """
        Execute function with comprehensive fault tolerance.
        
        Args:
            func: Function to execute
            circuit_name: Circuit breaker name
            fallback_key: Key for fallback strategies
            retry_config: Optional retry configuration
            *args: Function arguments
            **kwargs: Function keyword arguments
            
        Returns:
            Function result
        """
        circuit_breaker = self.circuit_manager.get_breaker(circuit_name)
        retry_mechanism = RetryMechanism(retry_config) if retry_config else self.retry_mechanism
        
        async def protected_func():
            return await circuit_breaker.call(func, *args, **kwargs)
        
        if fallback_key:
            async def retry_func():
                return await retry_mechanism.execute(protected_func)
            return await self.fallback_strategy.execute_with_fallback(
                retry_func,
                fallback_key
            )
        else:
            return await retry_mechanism.execute(protected_func)
    
    def get_system_health(self) -> Dict[str, Any]:
        """Get comprehensive system health status."""
        return {
            'circuit_breakers': self.circuit_manager.get_health_status(),
            'services': self.health_monitor.get_all_status(),
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def register_service_health_check(self, 
                                    name: str, 
                                    health_check_func: Callable,
                                    *args, **kwargs):
        """Register a service for health monitoring."""
        self.health_monitor.register_service(name, health_check_func, *args, **kwargs)
    
    def reset_all_circuits(self):
        """Reset all circuit breakers."""
        self.circuit_manager.reset_all()
    
    def shutdown(self):
        """Shutdown fault tolerance manager."""
        self.health_monitor.stop_all_monitoring()
        logger.info("Fault tolerance manager shutdown")


# Global fault tolerance manager instance
fault_tolerance_manager = FaultToleranceManager()


def fault_tolerant(circuit_name: str, 
                  fallback_key: str = None,
                  retry_config: RetryConfig = None):
    """
    Decorator for applying comprehensive fault tolerance to functions.
    
    Args:
        circuit_name: Circuit breaker name
        fallback_key: Optional fallback key
        retry_config: Optional retry configuration
        
    Returns:
        Decorated function
    """
    def decorator(func):
        async def wrapper(*args, **kwargs):
            return await fault_tolerance_manager.execute_with_fault_tolerance(
                func, circuit_name, fallback_key, retry_config, *args, **kwargs
            )
        
        wrapper.__name__ = func.__name__
        wrapper.__doc__ = func.__doc__
        return wrapper
    return decorator