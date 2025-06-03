"""
Advanced Flow Control for Reactive Streams

This module provides sophisticated flow control mechanisms for managing
data flow in reactive streams with dynamic rate adjustment and congestion control.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Callable
from dataclasses import dataclass, field
from enum import Enum
import time
import math
from collections import deque

logger = logging.getLogger(__name__)


class FlowControlState(Enum):
    """Flow control states."""
    NORMAL = "normal"
    CONGESTED = "congested"
    THROTTLED = "throttled"
    BLOCKED = "blocked"


class CongestionAlgorithm(Enum):
    """Congestion control algorithms."""
    AIMD = "aimd"  # Additive Increase Multiplicative Decrease
    CUBIC = "cubic"  # CUBIC congestion control
    BBR = "bbr"  # Bottleneck Bandwidth and Round-trip propagation time
    ADAPTIVE = "adaptive"  # Adaptive algorithm based on system metrics


@dataclass
class FlowControlConfig:
    """Configuration for flow control."""
    initial_rate: float = 100.0  # Initial processing rate (items/second)
    max_rate: float = 1000.0  # Maximum processing rate
    min_rate: float = 1.0  # Minimum processing rate
    congestion_algorithm: CongestionAlgorithm = CongestionAlgorithm.ADAPTIVE
    congestion_threshold: float = 0.8  # Trigger congestion control at 80% capacity
    recovery_factor: float = 0.5  # Factor to reduce rate during congestion
    increase_factor: float = 1.1  # Factor to increase rate during normal operation
    measurement_window: int = 100  # Window size for measurements
    enable_prediction: bool = True  # Enable predictive flow control
    prediction_horizon: int = 10  # Prediction horizon in seconds


@dataclass
class FlowMetrics:
    """Metrics for flow control monitoring."""
    current_rate: float = 0.0
    target_rate: float = 0.0
    actual_throughput: float = 0.0
    queue_length: int = 0
    congestion_events: int = 0
    rate_adjustments: int = 0
    average_latency: float = 0.0
    packet_loss_rate: float = 0.0
    bandwidth_utilization: float = 0.0


class FlowController:
    """
    Advanced flow controller with multiple congestion control algorithms.
    
    Provides intelligent flow control to optimize throughput while preventing
    system overload and maintaining low latency.
    """
    
    def __init__(self, config: FlowControlConfig = None):
        self.config = config or FlowControlConfig()
        self.state = FlowControlState.NORMAL
        self.metrics = FlowMetrics()
        
        # Rate control
        self.current_rate = self.config.initial_rate
        self.target_rate = self.config.initial_rate
        
        # Measurement windows
        self.throughput_window = deque(maxlen=self.config.measurement_window)
        self.latency_window = deque(maxlen=self.config.measurement_window)
        self.queue_length_window = deque(maxlen=self.config.measurement_window)
        
        # Congestion control state
        self.congestion_window = self.config.initial_rate
        self.slow_start_threshold = self.config.max_rate / 2
        self.in_slow_start = True
        
        # CUBIC algorithm state
        self.cubic_w_max = 0.0
        self.cubic_k = 0.0
        self.cubic_origin_point = 0.0
        self.cubic_epoch_start = 0.0
        
        # BBR algorithm state
        self.bbr_bandwidth = 0.0
        self.bbr_rtt = 0.0
        self.bbr_delivery_rate = 0.0
        
        # Adaptive algorithm state
        self.adaptive_history = deque(maxlen=50)
        self.prediction_model = None
        
        # Timing
        self.last_adjustment = time.time()
        self.last_measurement = time.time()
        
        logger.info(f"FlowController initialized with algorithm: {self.config.congestion_algorithm}")
    
    async def control_flow(
        self, 
        queue_length: int, 
        processing_latency: float,
        throughput: float
    ) -> float:
        """
        Control flow based on current system metrics.
        
        Args:
            queue_length: Current queue length
            processing_latency: Current processing latency
            throughput: Current throughput
            
        Returns:
            Recommended processing rate
        """
        # Update metrics
        self._update_metrics(queue_length, processing_latency, throughput)
        
        # Detect congestion
        congestion_detected = self._detect_congestion()
        
        # Apply congestion control algorithm
        if congestion_detected:
            await self._handle_congestion()
        else:
            await self._handle_normal_operation()
        
        # Apply rate limiting
        self.current_rate = max(
            self.config.min_rate,
            min(self.config.max_rate, self.target_rate)
        )
        
        # Update state
        self._update_state()
        
        return self.current_rate
    
    def _update_metrics(
        self, 
        queue_length: int, 
        processing_latency: float, 
        throughput: float
    ) -> None:
        """Update flow control metrics."""
        current_time = time.time()
        
        # Add to measurement windows
        self.throughput_window.append(throughput)
        self.latency_window.append(processing_latency)
        self.queue_length_window.append(queue_length)
        
        # Calculate averages
        if self.throughput_window:
            self.metrics.actual_throughput = sum(self.throughput_window) / len(self.throughput_window)
        
        if self.latency_window:
            self.metrics.average_latency = sum(self.latency_window) / len(self.latency_window)
        
        self.metrics.queue_length = queue_length
        self.metrics.current_rate = self.current_rate
        self.metrics.target_rate = self.target_rate
        
        # Calculate bandwidth utilization
        if self.config.max_rate > 0:
            self.metrics.bandwidth_utilization = throughput / self.config.max_rate
        
        self.last_measurement = current_time
    
    def _detect_congestion(self) -> bool:
        """Detect if congestion is occurring."""
        # Multiple congestion indicators
        indicators = []
        
        # Queue length indicator
        if self.metrics.queue_length > 0:
            queue_ratio = self.metrics.queue_length / (self.current_rate * 2)  # 2 seconds worth
            indicators.append(queue_ratio > self.config.congestion_threshold)
        
        # Latency indicator
        if len(self.latency_window) >= 10:
            recent_latency = sum(list(self.latency_window)[-10:]) / 10
            baseline_latency = sum(list(self.latency_window)[:10]) / 10 if len(self.latency_window) >= 20 else recent_latency
            if baseline_latency > 0:
                latency_increase = recent_latency / baseline_latency
                indicators.append(latency_increase > 1.5)  # 50% increase
        
        # Throughput indicator
        if len(self.throughput_window) >= 10:
            recent_throughput = sum(list(self.throughput_window)[-10:]) / 10
            expected_throughput = self.current_rate * 0.8  # 80% of target
            indicators.append(recent_throughput < expected_throughput)
        
        # Congestion if multiple indicators are true
        congestion_score = sum(indicators)
        return congestion_score >= 2
    
    async def _handle_congestion(self) -> None:
        """Handle congestion based on the configured algorithm."""
        self.state = FlowControlState.CONGESTED
        self.metrics.congestion_events += 1
        
        algorithm = self.config.congestion_algorithm
        
        if algorithm == CongestionAlgorithm.AIMD:
            await self._apply_aimd_congestion()
        elif algorithm == CongestionAlgorithm.CUBIC:
            await self._apply_cubic_congestion()
        elif algorithm == CongestionAlgorithm.BBR:
            await self._apply_bbr_congestion()
        elif algorithm == CongestionAlgorithm.ADAPTIVE:
            await self._apply_adaptive_congestion()
        
        logger.warning(f"Congestion detected - rate reduced to {self.target_rate:.2f}")
    
    async def _handle_normal_operation(self) -> None:
        """Handle normal operation - increase rate if possible."""
        if self.state == FlowControlState.CONGESTED:
            self.state = FlowControlState.NORMAL
        
        algorithm = self.config.congestion_algorithm
        
        if algorithm == CongestionAlgorithm.AIMD:
            await self._apply_aimd_normal()
        elif algorithm == CongestionAlgorithm.CUBIC:
            await self._apply_cubic_normal()
        elif algorithm == CongestionAlgorithm.BBR:
            await self._apply_bbr_normal()
        elif algorithm == CongestionAlgorithm.ADAPTIVE:
            await self._apply_adaptive_normal()
    
    async def _apply_aimd_congestion(self) -> None:
        """Apply AIMD congestion control - multiplicative decrease."""
        self.target_rate *= self.config.recovery_factor
        self.congestion_window = self.target_rate
        self.slow_start_threshold = self.target_rate
        self.in_slow_start = False
        self.metrics.rate_adjustments += 1
    
    async def _apply_aimd_normal(self) -> None:
        """Apply AIMD normal operation - additive increase."""
        if self.in_slow_start:
            # Exponential increase in slow start
            self.target_rate *= 2
            if self.target_rate >= self.slow_start_threshold:
                self.in_slow_start = False
        else:
            # Linear increase in congestion avoidance
            self.target_rate += 1.0  # Add 1 item/second
        
        self.congestion_window = self.target_rate
        self.metrics.rate_adjustments += 1
    
    async def _apply_cubic_congestion(self) -> None:
        """Apply CUBIC congestion control."""
        self.cubic_w_max = self.congestion_window
        self.congestion_window *= 0.7  # CUBIC reduction factor
        self.target_rate = self.congestion_window
        
        # Calculate K (time to reach w_max again)
        beta = 0.7
        C = 0.4
        self.cubic_k = math.pow((self.cubic_w_max * (1 - beta)) / C, 1/3)
        self.cubic_epoch_start = time.time()
        self.metrics.rate_adjustments += 1
    
    async def _apply_cubic_normal(self) -> None:
        """Apply CUBIC normal operation."""
        current_time = time.time()
        t = current_time - self.cubic_epoch_start
        
        # CUBIC function
        C = 0.4
        target = C * math.pow(t - self.cubic_k, 3) + self.cubic_w_max
        
        if target > self.congestion_window:
            self.congestion_window = target
            self.target_rate = self.congestion_window
            self.metrics.rate_adjustments += 1
    
    async def _apply_bbr_congestion(self) -> None:
        """Apply BBR congestion control."""
        # BBR doesn't react to packet loss, but we adapt for our use case
        self.target_rate = self.bbr_bandwidth * 0.8  # 80% of estimated bandwidth
        self.metrics.rate_adjustments += 1
    
    async def _apply_bbr_normal(self) -> None:
        """Apply BBR normal operation."""
        # Estimate bandwidth and RTT
        if self.throughput_window:
            self.bbr_bandwidth = max(self.throughput_window)
        
        if self.latency_window:
            self.bbr_rtt = min(self.latency_window)
        
        # Set rate based on bandwidth-delay product
        if self.bbr_rtt > 0:
            bdp = self.bbr_bandwidth * self.bbr_rtt
            self.target_rate = min(self.bbr_bandwidth, bdp * 2)  # 2x BDP
            self.metrics.rate_adjustments += 1
    
    async def _apply_adaptive_congestion(self) -> None:
        """Apply adaptive congestion control."""
        # Record current state for learning
        state = {
            'queue_length': self.metrics.queue_length,
            'latency': self.metrics.average_latency,
            'throughput': self.metrics.actual_throughput,
            'rate': self.current_rate,
            'timestamp': time.time()
        }
        self.adaptive_history.append(state)
        
        # Adaptive reduction based on severity
        severity = self._calculate_congestion_severity()
        reduction_factor = 0.5 + (0.4 * (1 - severity))  # 0.5 to 0.9
        self.target_rate *= reduction_factor
        self.metrics.rate_adjustments += 1
    
    async def _apply_adaptive_normal(self) -> None:
        """Apply adaptive normal operation."""
        # Predict optimal rate based on historical data
        if self.config.enable_prediction and len(self.adaptive_history) >= 10:
            predicted_rate = self._predict_optimal_rate()
            if predicted_rate:
                self.target_rate = predicted_rate
            else:
                # Fallback to conservative increase
                self.target_rate *= self.config.increase_factor
        else:
            # Conservative increase
            self.target_rate *= self.config.increase_factor
        
        self.metrics.rate_adjustments += 1
    
    def _calculate_congestion_severity(self) -> float:
        """Calculate congestion severity (0.0 to 1.0)."""
        severity_factors = []
        
        # Queue length factor
        if self.current_rate > 0:
            queue_factor = min(1.0, self.metrics.queue_length / (self.current_rate * 5))
            severity_factors.append(queue_factor)
        
        # Latency factor
        if len(self.latency_window) >= 2:
            recent_latency = self.latency_window[-1]
            baseline_latency = sum(self.latency_window) / len(self.latency_window)
            if baseline_latency > 0:
                latency_factor = min(1.0, (recent_latency / baseline_latency - 1.0) / 2.0)
                severity_factors.append(max(0.0, latency_factor))
        
        # Throughput factor
        if self.current_rate > 0:
            throughput_factor = 1.0 - (self.metrics.actual_throughput / self.current_rate)
            severity_factors.append(max(0.0, min(1.0, throughput_factor)))
        
        return sum(severity_factors) / len(severity_factors) if severity_factors else 0.0
    
    def _predict_optimal_rate(self) -> Optional[float]:
        """Predict optimal rate based on historical data."""
        if len(self.adaptive_history) < 20:
            return None
        
        # Simple linear regression on recent data
        recent_data = list(self.adaptive_history)[-20:]
        
        # Find correlation between rate and throughput
        rates = [d['rate'] for d in recent_data]
        throughputs = [d['throughput'] for d in recent_data]
        
        if len(set(rates)) < 2:  # Need variation in rates
            return None
        
        # Calculate correlation
        n = len(rates)
        sum_x = sum(rates)
        sum_y = sum(throughputs)
        sum_xy = sum(r * t for r, t in zip(rates, throughputs))
        sum_x2 = sum(r * r for r in rates)
        
        # Linear regression slope
        denominator = n * sum_x2 - sum_x * sum_x
        if denominator == 0:
            return None
        
        slope = (n * sum_xy - sum_x * sum_y) / denominator
        
        # Predict rate that maximizes throughput
        if slope > 0:
            # Positive correlation - can increase rate
            return min(self.config.max_rate, self.current_rate * 1.2)
        else:
            # Negative correlation - should decrease rate
            return max(self.config.min_rate, self.current_rate * 0.9)
    
    def _update_state(self) -> None:
        """Update flow control state based on current conditions."""
        if self.metrics.queue_length > self.current_rate * 10:  # 10 seconds worth
            self.state = FlowControlState.BLOCKED
        elif self.metrics.bandwidth_utilization > 0.9:
            self.state = FlowControlState.THROTTLED
        elif self._detect_congestion():
            self.state = FlowControlState.CONGESTED
        else:
            self.state = FlowControlState.NORMAL
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get flow control metrics."""
        return {
            "state": self.state.value,
            "algorithm": self.config.congestion_algorithm.value,
            "current_rate": round(self.current_rate, 2),
            "target_rate": round(self.target_rate, 2),
            "actual_throughput": round(self.metrics.actual_throughput, 2),
            "queue_length": self.metrics.queue_length,
            "congestion_events": self.metrics.congestion_events,
            "rate_adjustments": self.metrics.rate_adjustments,
            "average_latency_ms": round(self.metrics.average_latency * 1000, 2),
            "bandwidth_utilization": round(self.metrics.bandwidth_utilization, 3),
            "efficiency": round(self.metrics.actual_throughput / max(self.current_rate, 1), 3)
        }
    
    def reset_metrics(self) -> None:
        """Reset flow control metrics."""
        self.metrics = FlowMetrics()
        self.throughput_window.clear()
        self.latency_window.clear()
        self.queue_length_window.clear()
        self.adaptive_history.clear()
        logger.info("Flow control metrics reset")
    
    def is_healthy(self) -> bool:
        """Check if flow controller is healthy."""
        return (
            self.state != FlowControlState.BLOCKED and
            self.metrics.bandwidth_utilization < 0.95 and
            self.current_rate >= self.config.min_rate
        )