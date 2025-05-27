import asyncio
import logging
import time
from typing import Dict, List, Any, Optional

from pyraft.core.constants import NodeState
from pyraft.utils.network_metrics import NetworkMetrics
from pyraft.utils.partition_detector import PartitionState


class PartitionRecovery:
    """Handle recovery procedures when network partitions heal."""
    
    def __init__(self, node):
        """
        Initialize a new PartitionRecovery instance.
        
        Args:
            node: The RaftNode instance to handle recovery for.
        """
        self.node = node
        self.logger = logging.getLogger(f"raft.recovery.{node.node_id}")
        self.recovery_in_progress = False
        self.last_recovery_time = 0
        self.min_recovery_interval = 30.0  # seconds
    
    async def handle_partition_recovery(self):
        """
        Handle recovery when a network partition heals.
        
        This method implements a comprehensive recovery procedure:
        1. Reset failure counters and partition state
        2. Step down if we were a leader in a minority partition
        3. Discover the current leader if we're a follower
        4. Reset adaptive timeouts to baseline
        5. Sync log with the cluster if needed
        """
        current_time = time.time()
        if self.recovery_in_progress or (current_time - self.last_recovery_time < self.min_recovery_interval):
            return
        
        self.recovery_in_progress = True
        self.last_recovery_time = current_time
        
        try:
            self.logger.info("Starting partition recovery procedures")
            old_partition_state = self.node.partition_state
            
            self.node.partition_detector.consecutive_failures.clear()
            self.node.partition_detector.unreachable_nodes.clear()
            self.node.partition_state = PartitionState.CONNECTED
            
            self.logger.info(f"Partition state changed from {old_partition_state.value} to {self.node.partition_state.value}")
            
            if (self.node.state == NodeState.LEADER and 
                old_partition_state in [PartitionState.MINORITY_PARTITION, PartitionState.ISOLATED]):
                self.logger.warning("Was leader in minority partition, stepping down")
                await self.node._step_down_from_leadership()
            
            if self.node.state == NodeState.FOLLOWER:
                self.logger.info("Attempting to discover current leader after partition recovery")
                await self._discover_leader()
            
            self.node.network_metrics = NetworkMetrics()
            
            if self.node.state == NodeState.FOLLOWER and self.node.leader_id:
                await self._request_log_sync()
            
            self.logger.info("Partition recovery procedures completed")
        except Exception as e:
            self.logger.error(f"Error during partition recovery: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
        finally:
            self.recovery_in_progress = False
    
    async def _discover_leader(self):
        """
        Attempt to discover the current leader after partition recovery.
        
        This method contacts other nodes in the cluster to find the current leader.
        """
        for node_id in self.node.cluster_config:
            if node_id == self.node.node_id:
                continue
            
            try:
                args = {
                    'term': self.node.current_term,
                    'find_leader': True,
                    'recovery': True
                }
                
                if self.node.send_append_entries_callback:
                    response = await self.node.send_append_entries_callback(node_id, args)
                    
                    if response and response.get('leader_id'):
                        self.node.leader_id = response.get('leader_id')
                        self.logger.info(f"Discovered leader: {self.node.leader_id}")
                        return
                        
            except Exception as e:
                self.logger.debug(f"Error contacting {node_id} during leader discovery: {e}")
                continue
    
    async def _request_log_sync(self):
        """
        Request a log sync from the leader to ensure we have the latest entries.
        
        This is important after a partition heals to quickly catch up with missed entries.
        """
        if not self.node.leader_id or not self.node.send_append_entries_callback:
            return
        
        try:
            self.logger.info(f"Requesting log sync from leader {self.node.leader_id}")
            
            args = {
                'term': self.node.current_term,
                'sync_request': True,
                'last_log_index': self.node.log.last_index,
                'last_log_term': self.node.log.last_term
            }
            
            response = await self.node.send_append_entries_callback(self.node.leader_id, args)
            
            if response and response.get('success'):
                self.logger.info("Log sync request acknowledged by leader")
            else:
                self.logger.warning(f"Log sync request failed: {response}")
                
        except Exception as e:
            self.logger.error(f"Error requesting log sync: {e}")
    
    def check_partition_healing(self):
        """
        Check if a partition appears to be healing based on recent successful contacts.
        
        Returns:
            bool: True if the partition appears to be healing, False otherwise.
        """
        if self.node.partition_state == PartitionState.CONNECTED:
            return False
        
        unreachable_nodes = set(self.node.partition_detector.unreachable_nodes)
        if not unreachable_nodes:
            return False
        
        recently_reachable = 0
        for node_id in unreachable_nodes:
            if self.node.network_metrics.is_node_responsive(node_id, max_age=10.0):
                recently_reachable += 1
        
        return recently_reachable >= len(unreachable_nodes) / 2
