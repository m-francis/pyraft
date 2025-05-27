from enum import Enum, auto
from typing import Dict, List, Optional, Set, Tuple, Any, Callable, Awaitable
import asyncio
import random
import time
import logging

from pyraft.core.log import RaftLog
from pyraft.core.state_machine import StateMachine


class NodeState(Enum):
    """
    Represents the possible states of a Raft node.
    """
    FOLLOWER = auto()
    CANDIDATE = auto()
    LEADER = auto()


class RaftNode:
    """
    Represents a node in a Raft cluster.
    
    This class implements the core Raft consensus algorithm, including leader election,
    log replication, and safety properties.
    """
    
    def __init__(
        self,
        node_id: str,
        cluster_config: Dict[str, Dict[str, Any]],
        state_machine: StateMachine,
        storage_dir: Optional[str] = None,
        election_timeout_min: float = 5000,  # Increased from 1500ms to 5000ms
        election_timeout_max: float = 10000, # Increased from 3000ms to 10000ms
        heartbeat_interval: float = 1000,    # Increased from 500ms to 1000ms
        test_mode: bool = False
    ):
        """
        Initialize a new Raft node.
        
        Args:
            node_id: Unique identifier for this node.
            cluster_config: Dictionary mapping node IDs to their configuration.
            state_machine: The state machine to apply committed commands to.
            storage_dir: Directory to store persistent state. If None, state is not persisted.
            election_timeout_min: Minimum election timeout in milliseconds.
            election_timeout_max: Maximum election timeout in milliseconds.
            heartbeat_interval: Heartbeat interval in milliseconds.
            test_mode: Whether to run in test mode (skips creating asyncio tasks).
        """
        self.node_id = node_id
        self.cluster_config = cluster_config
        self.state_machine = state_machine
        self.storage_dir = storage_dir
        
        self.current_term = 0
        self.voted_for = None
        self.log = RaftLog(storage_dir)
        
        self.state = NodeState.FOLLOWER
        self.commit_index = 0
        self.last_applied = 0
        
        self.next_index = {}
        self.match_index = {}
        
        self.leader_id = None
        
        self.election_timeout_min = election_timeout_min / 1000  # Convert to seconds
        self.election_timeout_max = election_timeout_max / 1000
        self.heartbeat_interval = heartbeat_interval / 1000
        
        self.election_timer = None
        self.heartbeat_timer = None
        
        self.send_append_entries_callback: Optional[Callable[[str, Dict[str, Any]], Awaitable[Dict[str, Any]]]] = None
        self.send_request_vote_callback: Optional[Callable[[str, Dict[str, Any]], Awaitable[Dict[str, Any]]]] = None
        self.send_client_response_callback: Optional[Callable[[str, Dict[str, Any]], Awaitable[Dict[str, Any]]]] = None
        
        self.logger = logging.getLogger(f"raft.node.{node_id}")
        
        self.test_mode = test_mode
        
        self._load_persistent_state()
        
        if not test_mode:
            self._reset_election_timer()
    
    def _load_persistent_state(self) -> None:
        """
        Load persistent state from disk.
        """
        
        if self.storage_dir:
            import os
            import json
            
            state_path = os.path.join(self.storage_dir, "state.json")
            if os.path.exists(state_path):
                with open(state_path, 'r') as f:
                    state = json.load(f)
                    self.current_term = state.get('current_term', 0)
                    self.voted_for = state.get('voted_for')
    
    def _save_persistent_state(self) -> None:
        """
        Save persistent state to disk.
        """
        if self.storage_dir:
            import os
            import json
            
            state = {
                'current_term': self.current_term,
                'voted_for': self.voted_for
            }
            
            state_path = os.path.join(self.storage_dir, "state.json")
            with open(state_path, 'w') as f:
                json.dump(state, f)
    
    def _reset_election_timer(self) -> None:
        """
        Reset the election timeout with a random duration.
        """
        if self.election_timer:
            self.election_timer.cancel()
            
        timeout = random.uniform(self.election_timeout_min, self.election_timeout_max)
        
        if not self.test_mode:
            try:
                self.election_timer = asyncio.create_task(self._election_timeout(timeout))
            except RuntimeError:
                self.logger.warning("No running event loop, skipping election timer")
    
    async def _election_timeout(self, timeout: float) -> None:
        """
        Handle election timeout.
        
        Args:
            timeout: The timeout duration in seconds.
        """
        await asyncio.sleep(timeout)
        self.logger.info(f"Election timeout triggered, starting election for term {self.current_term + 1}")
        await self._start_election()
    
    async def _start_election(self) -> None:
        """
        Start a new election.
        """
        self.logger.info(f"Election timeout triggered, starting election for term {self.current_term + 1}")
        
        if len(self.cluster_config) == 1:
            self.current_term += 1
            self.state = NodeState.CANDIDATE
            self.voted_for = self.node_id
            self._save_persistent_state()
            self.logger.warning(f"Single-node cluster, becoming leader for term {self.current_term}")
            await self._become_leader()
            return
            
        self.current_term += 1
        self.state = NodeState.CANDIDATE
        self.voted_for = self.node_id
        self._save_persistent_state()
        
        last_log_term, last_log_index = self.log.get_last_log_term_and_index()
        
        # Vote for self
        votes_received = 1
        connection_errors = 0
        
        for node_id in self.cluster_config:
            if node_id == self.node_id:
                continue
                
            args = {
                'term': self.current_term,
                'candidate_id': self.node_id,
                'last_log_index': last_log_index,
                'last_log_term': last_log_term
            }
            
            if self.send_request_vote_callback:
                try:
                    self.logger.debug(f"Requesting vote from {node_id} for term {self.current_term}")
                    response = await self.send_request_vote_callback(node_id, args)
                    
                    if response and response.get('error') in ['connection_error', 'connection_refused', 'timeout']:
                        self.logger.warning(f"Connection error to {node_id} during election: {response.get('error')}")
                        connection_errors += 1
                        continue
                    
                    self.logger.debug(f"Received vote response from {node_id}: {response}")
                    
                    if response and response.get('term', 0) > self.current_term:
                        self.logger.info(f"Discovered higher term {response.get('term')} from {node_id}, reverting to follower")
                        self.current_term = response.get('term')
                        self.state = NodeState.FOLLOWER
                        self.voted_for = None
                        self._save_persistent_state()
                        self._reset_election_timer()
                        return
                        
                    if response and response.get('vote_granted', False):
                        self.logger.info(f"Received vote from {node_id} for term {self.current_term}")
                        votes_received += 1
                        
                        if votes_received > len(self.cluster_config) / 2:
                            self.logger.info(f"Won election for term {self.current_term} with {votes_received} votes")
                            await self._become_leader()
                            return
                except Exception as e:
                    self.logger.error(f"Error sending RequestVote to {node_id}: {e}")
                    connection_errors += 1
        
        if connection_errors == len(self.cluster_config) - 1:
            self.logger.warning(f"No other nodes reachable, operating in single-node mode for term {self.current_term}")
            await self._become_leader()
            return
            
        self.logger.info(f"Election for term {self.current_term} inconclusive with {votes_received} votes, waiting for timeout")
        self._reset_election_timer()
    
    async def _become_leader(self) -> None:
        """
        Transition to leader state.
        """
        if self.state != NodeState.CANDIDATE:
            return
            
        self.logger.info(f"Node {self.node_id} becoming leader for term {self.current_term}")
        
        self.state = NodeState.LEADER
        self.leader_id = self.node_id  # Set self as leader
        
        for node_id in self.cluster_config:
            if node_id != self.node_id:
                _, last_log_index = self.log.get_last_log_term_and_index()
                self.next_index[node_id] = last_log_index + 1
                
                self.match_index[node_id] = 0
        
        if self.election_timer:
            self.election_timer.cancel()
            self.election_timer = None
        
        await self._send_heartbeats()
        
        if not self.test_mode:
            try:
                self.heartbeat_timer = asyncio.create_task(self._heartbeat_loop())
            except RuntimeError:
                self.logger.warning("No running event loop, skipping heartbeat timer")
    
    async def _heartbeat_loop(self) -> None:
        """
        Periodically send heartbeats to maintain leadership.
        """
        while self.state == NodeState.LEADER:
            await self._send_heartbeats()
            await asyncio.sleep(self.heartbeat_interval)
    
    async def _send_heartbeats(self) -> None:
        """
        Send AppendEntries RPCs with empty entries to all followers.
        """
        for node_id in self.cluster_config:
            if node_id == self.node_id:
                continue
                
            await self._send_append_entries(node_id)
    
    async def _send_append_entries(self, node_id: str) -> None:
        """
        Send AppendEntries RPC to a follower.
        
        Args:
            node_id: The ID of the follower to send to.
        """
        if self.state != NodeState.LEADER:
            return
            
        next_idx = self.next_index.get(node_id, 1)
        
        prev_log_index = next_idx - 1
        prev_log_term = 0
        
        if prev_log_index > 0:
            prev_entry = self.log.get_entry(prev_log_index)
            if prev_entry:
                prev_log_term = prev_entry.term
        
        entries = self.log.get_entries_from(next_idx)
        
        args = {
            'term': self.current_term,
            'leader_id': self.node_id,
            'prev_log_index': prev_log_index,
            'prev_log_term': prev_log_term,
            'entries': [{'term': e.term, 'command': e.command, 'index': e.index} for e in entries],
            'leader_commit': self.commit_index
        }
        
        if self.send_append_entries_callback:
            try:
                response = await self.send_append_entries_callback(node_id, args)
            except Exception as e:
                self.logger.error(f"Error sending AppendEntries to {node_id}: {e}")
                response = {'error': 'connection_error'}
            
            if response:
                if response.get('term', 0) > self.current_term:
                    self.current_term = response['term']
                    self.state = NodeState.FOLLOWER
                    self.voted_for = None
                    self._save_persistent_state()
                    self._reset_election_timer()
                    
                    if self.heartbeat_timer:
                        self.heartbeat_timer.cancel()
                        self.heartbeat_timer = None
                        
                    return
                
                if response.get('success', False):
                    if entries:
                        self.match_index[node_id] = entries[-1].index
                        self.next_index[node_id] = entries[-1].index + 1
                    
                    self._update_commit_index()
                else:
                    self.next_index[node_id] = max(1, self.next_index[node_id] - 1)
                    
                    await self._send_append_entries(node_id)
    
    def _update_commit_index(self) -> None:
        """
        Update the commit index if there exists an N such that:
        - N > commitIndex
        - A majority of matchIndex[i] ≥ N
        - log[N].term == currentTerm
        """
        if self.state != NodeState.LEADER:
            return
            
        log_size = self.log.get_size()
        
        for N in range(self.commit_index + 1, log_size + 1):
            entry = self.log.get_entry(N)
            if not entry or entry.term != self.current_term:
                continue
                
            count = 1  # Leader has the entry
            for node_id, match_idx in self.match_index.items():
                if match_idx >= N:
                    count += 1
            
            if count > len(self.cluster_config) / 2:
                self.commit_index = N
            else:
                break
        
        self._apply_committed_entries()
    
    def _apply_committed_entries(self) -> None:
        """
        Apply committed but not yet applied entries to the state machine.
        """
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log.get_entry(self.last_applied)
            if entry:
                result = self.state_machine.apply(entry.command)
                self.logger.debug(f"Applied entry {self.last_applied} to state machine: {entry.command}")
    
    async def handle_append_entries(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle an AppendEntries RPC from a leader.
        
        Args:
            args: The AppendEntries arguments.
            
        Returns:
            The AppendEntries response.
        """
        if args.get('find_leader', False):
            self.logger.info(f"Received find_leader request, current leader: {self.leader_id}")
            return {
                'term': self.current_term,
                'success': True,
                'leader_id': self.leader_id
            }
            
        term = args.get('term', 0)
        leader_id = args.get('leader_id', '')
        prev_log_index = args.get('prev_log_index', 0)
        prev_log_term = args.get('prev_log_term', 0)
        entries = args.get('entries', [])
        leader_commit = args.get('leader_commit', 0)
        
        response = {
            'term': self.current_term,
            'success': False
        }
        
        if term < self.current_term:
            return response
        
        self._reset_election_timer()
        
        if self.state != NodeState.FOLLOWER:
            self.state = NodeState.FOLLOWER
            
            if self.heartbeat_timer:
                self.heartbeat_timer.cancel()
                self.heartbeat_timer = None
        
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            self._save_persistent_state()
        
        if leader_id:
            self.leader_id = leader_id
            self.logger.info(f"Updated leader to {leader_id} for term {term}")
        
        if prev_log_index > 0:
            prev_entry = self.log.get_entry(prev_log_index)
            if not prev_entry or prev_entry.term != prev_log_term:
                return response
        
        for entry_data in entries:
            index = entry_data.get('index', 0)
            term = entry_data.get('term', 0)
            
            existing_entry = self.log.get_entry(index)
            if existing_entry and existing_entry.term != term:
                self.log.delete_entries_from(index)
                break
        
        for entry_data in entries:
            index = entry_data.get('index', 0)
            term = entry_data.get('term', 0)
            command = entry_data.get('command', {})
            
            existing_entry = self.log.get_entry(index)
            if not existing_entry:
                if index == self.log.get_size() + 1:
                    self.log.append(term, command)
        
        if leader_commit > self.commit_index:
            last_new_entry_index = entries[-1]['index'] if entries else prev_log_index
            self.commit_index = min(leader_commit, last_new_entry_index)
            self._apply_committed_entries()
        
        response['success'] = True
        return response
    
    async def handle_request_vote(self, args: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle a RequestVote RPC from a candidate.
        
        Args:
            args: The RequestVote arguments.
            
        Returns:
            The RequestVote response.
        """
        term = args.get('term', 0)
        candidate_id = args.get('candidate_id', '')
        last_log_index = args.get('last_log_index', 0)
        last_log_term = args.get('last_log_term', 0)
        
        response = {
            'term': self.current_term,
            'vote_granted': False
        }
        
        if term > self.current_term:
            self.current_term = term
            self.state = NodeState.FOLLOWER
            self.voted_for = None
            self._save_persistent_state()
            
            if self.heartbeat_timer:
                self.heartbeat_timer.cancel()
                self.heartbeat_timer = None
        
        if term < self.current_term:
            return response
        
        if (self.voted_for is None or self.voted_for == candidate_id):
            our_last_log_term, our_last_log_index = self.log.get_last_log_term_and_index()
            
            log_is_up_to_date = False
            if last_log_term > our_last_log_term:
                log_is_up_to_date = True
            elif last_log_term == our_last_log_term and last_log_index >= our_last_log_index:
                log_is_up_to_date = True
                
            if log_is_up_to_date:
                self.voted_for = candidate_id
                self._save_persistent_state()
                response['vote_granted'] = True
                
                self._reset_election_timer()
        
        return response
    
    async def handle_client_request(self, command: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle a request from a client.
        
        Args:
            command: The command to execute.
            
        Returns:
            A response to the client.
        """
        if command.get('action') == 'find_leader':
            self.logger.info(f"Handling find_leader request, current leader: {self.leader_id}")
            return {
                'success': True,
                'leader_id': self.leader_id,
                'current_term': self.current_term,
                'is_leader': self.state == NodeState.LEADER
            }
            
        if self.state != NodeState.LEADER:
            leader_id = None
            for node_id in self.cluster_config:
                if node_id != self.node_id:
                    args = {
                        'client_request': True
                    }
                    if self.send_append_entries_callback:
                        try:
                            response = await self.send_append_entries_callback(node_id, args)
                        except Exception as e:
                            self.logger.error(f"Error sending client request to {node_id}: {e}")
                            response = {'error': 'connection_error'}
                        if response and response.get('leader_id'):
                            leader_id = response.get('leader_id')
                            break
            
            return {
                'success': False,
                'error': 'not_leader',
                'leader_id': leader_id
            }
        
        entry = self.log.append(self.current_term, command)
        
        for node_id in self.cluster_config:
            if node_id != self.node_id:
                await self._send_append_entries(node_id)
        
        while self.commit_index < entry.index:
            if self.state != NodeState.LEADER:
                return {
                    'success': False,
                    'error': 'leadership_lost'
                }
                
            self._update_commit_index()
            
            if self.commit_index < entry.index:
                await asyncio.sleep(0.01)
        
        result = self.state_machine.apply(command)
        
        return {
            'success': True,
            'result': result
        }
    
    async def handle_read_request(self, command: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle a read request from a client with linearizable semantics.
        
        This implements the read-after-write guarantee by:
        1. Leader appends a no-op entry to the log and waits for it to be committed
        2. Leader then serves the read from its state machine
        
        Args:
            command: The read command.
            
        Returns:
            A response to the client.
        """
        self.logger.info(f"Handling read request: {command}")
        
        if self.state != NodeState.LEADER:
            leader_id = None
            for node_id in self.cluster_config:
                if node_id != self.node_id:
                    args = {
                        'client_request': True
                    }
                    if self.send_append_entries_callback:
                        try:
                            response = await self.send_append_entries_callback(node_id, args)
                            if response and response.get('leader_id'):
                                leader_id = response.get('leader_id')
                                break
                        except Exception as e:
                            self.logger.error(f"Error finding leader from {node_id}: {e}")
            
            self.logger.warning(f"Not leader for read request, redirecting to {leader_id}")
            return {
                'success': False,
                'error': 'not_leader',
                'leader_id': leader_id
            }
        
        if not command.get('linearizable', True):
            self.logger.info("Processing non-linearizable read request")
            try:
                result = self.state_machine.apply(command)
                return {
                    'success': True,
                    'result': result,
                    'index': self.commit_index
                }
            except Exception as e:
                self.logger.error(f"Error applying read command: {e}")
                return {
                    'success': False,
                    'error': f'state_machine_error: {str(e)}'
                }
        
        try:
            self.logger.info("Processing linearizable read request with no-op entry")
            no_op = {'action': 'no_op'}
            entry = self.log.append(self.current_term, no_op)
            
            for node_id in self.cluster_config:
                if node_id != self.node_id:
                    try:
                        await self._send_append_entries(node_id)
                    except Exception as e:
                        self.logger.error(f"Error sending append entries to {node_id}: {e}")
            
            # Wait for the no-op entry to be committed
            timeout = 5.0  # seconds
            start_time = time.time()
            
            while self.commit_index < entry.index:
                if self.state != NodeState.LEADER:
                    self.logger.warning("Lost leadership while waiting for no-op commit")
                    return {
                        'success': False,
                        'error': 'leadership_lost'
                    }
                
                if time.time() - start_time > timeout:
                    self.logger.warning(f"Timeout waiting for no-op commit: {self.commit_index} < {entry.index}")
                    return {
                        'success': False,
                        'error': 'commit_timeout'
                    }
                    
                self._update_commit_index()
                
                if self.commit_index < entry.index:
                    await asyncio.sleep(0.1)  # Increased sleep time
            
            self.logger.info(f"No-op committed, applying read command: {command}")
            result = self.state_machine.apply(command)
            
            return {
                'success': True,
                'result': result,
                'index': self.commit_index
            }
        except Exception as e:
            self.logger.error(f"Error handling read request: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return {
                'success': False,
                'error': f'server_error: {str(e)}'
            }
