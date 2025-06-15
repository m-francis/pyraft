import time
import logging
from typing import Dict, Set, List
from enum import Enum

class PartitionState(Enum):
    CONNECTED = "connected"
    MINORITY_PARTITION = "minority_partition"
    MAJORITY_PARTITION = "majority_partition"
    ISOLATED = "isolated"

class PartitionDetector:
    """Detect network partition scenarios."""
    
    def __init__(self, cluster_size: int, detection_threshold: int = 3):
        self.cluster_size = cluster_size
        self.detection_threshold = detection_threshold  # consecutive failures to trigger detection
        self.consecutive_failures: Dict[str, int] = {}
        self.last_contact: Dict[str, float] = {}
        self.unreachable_nodes: Set[str] = set()
        self.logger = logging.getLogger("raft.partition_detector")
    
    def record_success(self, node_id: str):
        """Record successful contact with a node."""
        self.consecutive_failures[node_id] = 0
        self.last_contact[node_id] = time.time()
        if node_id in self.unreachable_nodes:
            self.unreachable_nodes.remove(node_id)
            self.logger.info(f"Node {node_id} is now reachable again")
    
    def record_failure(self, node_id: str):
        """Record failed contact with a node."""
        self.consecutive_failures[node_id] = self.consecutive_failures.get(node_id, 0) + 1
        
        if self.consecutive_failures[node_id] >= self.detection_threshold:
            if node_id not in self.unreachable_nodes:
                self.unreachable_nodes.add(node_id)
                self.logger.warning(f"Node {node_id} appears unreachable after {self.consecutive_failures[node_id]} failures")
    
    def get_partition_state(self, current_node_id: str) -> PartitionState:
        """Determine the current partition state."""
        reachable_count = self.cluster_size - len(self.unreachable_nodes)
        
        if len(self.unreachable_nodes) == 0:
            return PartitionState.CONNECTED
        elif reachable_count == 1:  # Only this node
            return PartitionState.ISOLATED
        elif reachable_count > self.cluster_size // 2:
            return PartitionState.MAJORITY_PARTITION
        else:
            return PartitionState.MINORITY_PARTITION
    
    def get_reachable_nodes(self, all_nodes: List[str], current_node_id: str) -> List[str]:
        """Get list of currently reachable nodes."""
        return [node for node in all_nodes if node != current_node_id and node not in self.unreachable_nodes]
    
    def should_step_down(self, current_node_id: str) -> bool:
        """Check if a leader should step down due to partition."""
        state = self.get_partition_state(current_node_id)
        return state in [PartitionState.MINORITY_PARTITION, PartitionState.ISOLATED]
