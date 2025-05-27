import time
import asyncio
from typing import Dict, List, Optional
from collections import deque
import statistics
import logging

class NetworkMetrics:
    """Track network performance metrics for adaptive timeout calculation."""
    
    def __init__(self, window_size: int = 50):
        self.window_size = window_size
        self.rtt_samples: Dict[str, deque] = {}
        self.failure_counts: Dict[str, int] = {}
        self.total_requests: Dict[str, int] = {}
        self.last_success: Dict[str, float] = {}
        self.logger = logging.getLogger("raft.network_metrics")
    
    def record_rtt(self, node_id: str, rtt: float):
        """Record round-trip time for a successful request."""
        if node_id not in self.rtt_samples:
            self.rtt_samples[node_id] = deque(maxlen=self.window_size)
        self.rtt_samples[node_id].append(rtt)
        self.last_success[node_id] = time.time()
    
    def record_failure(self, node_id: str):
        """Record a failed request to a node."""
        self.failure_counts[node_id] = self.failure_counts.get(node_id, 0) + 1
        self.total_requests[node_id] = self.total_requests.get(node_id, 0) + 1
    
    def record_success(self, node_id: str):
        """Record a successful request to a node."""
        self.total_requests[node_id] = self.total_requests.get(node_id, 0) + 1
        self.last_success[node_id] = time.time()
    
    def get_avg_rtt(self, node_id: str) -> Optional[float]:
        """Get average RTT for a node."""
        if node_id in self.rtt_samples and self.rtt_samples[node_id]:
            return statistics.mean(self.rtt_samples[node_id])
        return None
    
    def get_failure_rate(self, node_id: str) -> float:
        """Get failure rate for a node."""
        total = self.total_requests.get(node_id, 0)
        if total == 0:
            return 0.0
        failures = self.failure_counts.get(node_id, 0)
        return failures / total
    
    def is_node_responsive(self, node_id: str, max_age: float = 30.0) -> bool:
        """Check if node has responded recently."""
        last_success = self.last_success.get(node_id)
        if last_success is None:
            return False
        return time.time() - last_success < max_age
    
    def get_adaptive_timeout(self, base_timeout: float, node_id: str = None) -> float:
        """Calculate adaptive timeout based on network conditions."""
        if node_id:
            avg_rtt = self.get_avg_rtt(node_id)
            failure_rate = self.get_failure_rate(node_id)
            
            if avg_rtt:
                multiplier = 1.0 + (failure_rate * 2.0)  # Up to 3x timeout for high failure rates
                return max(base_timeout, avg_rtt * 5 * multiplier)
        
        return base_timeout
