import unittest
import asyncio
import os
import sys
import tempfile
import shutil
import logging
import json
from unittest.mock import patch, MagicMock, AsyncMock

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from pyraft.core.node import RaftNode, NodeState
from pyraft.core.log import RaftLog, LogEntry
from pyraft.core.state_machine import DictStateMachine
from pyraft.client.client import RaftClient, RaftCounter


class TestRaftLog(unittest.TestCase):
    """Test the RaftLog class."""
    
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()
    
    def tearDown(self):
        shutil.rmtree(self.temp_dir)
    
    def test_append_and_get(self):
        """Test appending entries to the log and retrieving them."""
        log = RaftLog()
        
        entry1 = log.append(1, {'action': 'set', 'key': 'foo', 'value': 'bar'})
        entry2 = log.append(1, {'action': 'set', 'key': 'baz', 'value': 'qux'})
        
        self.assertEqual(entry1.term, 1)
        self.assertEqual(entry1.index, 1)
        self.assertEqual(entry1.command, {'action': 'set', 'key': 'foo', 'value': 'bar'})
        
        self.assertEqual(entry2.term, 1)
        self.assertEqual(entry2.index, 2)
        self.assertEqual(entry2.command, {'action': 'set', 'key': 'baz', 'value': 'qux'})
        
        retrieved_entry1 = log.get_entry(1)
        retrieved_entry2 = log.get_entry(2)
        
        self.assertEqual(retrieved_entry1, entry1)
        self.assertEqual(retrieved_entry2, entry2)
        
        self.assertIsNone(log.get_entry(3))
    
    def test_get_entries_from(self):
        """Test getting entries from a specific index."""
        log = RaftLog()
        
        entry1 = log.append(1, {'action': 'set', 'key': 'foo', 'value': 'bar'})
        entry2 = log.append(1, {'action': 'set', 'key': 'baz', 'value': 'qux'})
        entry3 = log.append(2, {'action': 'set', 'key': 'quux', 'value': 'corge'})
        
        entries = log.get_entries_from(2)
        self.assertEqual(len(entries), 2)
        self.assertEqual(entries[0], entry2)
        self.assertEqual(entries[1], entry3)
        
        entries = log.get_entries_from(3)
        self.assertEqual(len(entries), 1)
        self.assertEqual(entries[0], entry3)
        
        entries = log.get_entries_from(4)
        self.assertEqual(len(entries), 0)
    
    def test_get_last_log_term_and_index(self):
        """Test getting the term and index of the last entry."""
        log = RaftLog()
        
        term, index = log.get_last_log_term_and_index()
        self.assertEqual(term, 0)
        self.assertEqual(index, 0)
        
        log.append(1, {'action': 'set', 'key': 'foo', 'value': 'bar'})
        log.append(1, {'action': 'set', 'key': 'baz', 'value': 'qux'})
        log.append(2, {'action': 'set', 'key': 'quux', 'value': 'corge'})
        
        term, index = log.get_last_log_term_and_index()
        self.assertEqual(term, 2)
        self.assertEqual(index, 3)
    
    def test_delete_entries_from(self):
        """Test deleting entries from a specific index."""
        log = RaftLog()
        
        log.append(1, {'action': 'set', 'key': 'foo', 'value': 'bar'})
        log.append(1, {'action': 'set', 'key': 'baz', 'value': 'qux'})
        log.append(2, {'action': 'set', 'key': 'quux', 'value': 'corge'})
        
        log.delete_entries_from(2)
        
        self.assertEqual(log.get_size(), 1)
        self.assertIsNotNone(log.get_entry(1))
        self.assertIsNone(log.get_entry(2))
        self.assertIsNone(log.get_entry(3))
    
    def test_persistence(self):
        """Test log persistence."""
        log1 = RaftLog(self.temp_dir)
        
        log1.append(1, {'action': 'set', 'key': 'foo', 'value': 'bar'})
        log1.append(1, {'action': 'set', 'key': 'baz', 'value': 'qux'})
        log1.append(2, {'action': 'set', 'key': 'quux', 'value': 'corge'})
        
        log2 = RaftLog(self.temp_dir)
        
        self.assertEqual(log2.get_size(), 3)
        self.assertEqual(log2.get_entry(1).command, {'action': 'set', 'key': 'foo', 'value': 'bar'})
        self.assertEqual(log2.get_entry(2).command, {'action': 'set', 'key': 'baz', 'value': 'qux'})
        self.assertEqual(log2.get_entry(3).command, {'action': 'set', 'key': 'quux', 'value': 'corge'})


class TestDictStateMachine(unittest.TestCase):
    """Test the DictStateMachine class."""
    
    def test_apply(self):
        """Test applying commands to the state machine."""
        sm = DictStateMachine()
        
        result = sm.apply({'action': 'set', 'key': 'foo', 'value': 'bar'})
        self.assertEqual(result, 'bar')
        
        result = sm.apply({'action': 'get', 'key': 'foo'})
        self.assertEqual(result, 'bar')
        
        result = sm.apply({'action': 'get', 'key': 'baz'})
        self.assertIsNone(result)
        
        result = sm.apply({'action': 'delete', 'key': 'foo'})
        self.assertEqual(result, 'bar')
        
        result = sm.apply({'action': 'get', 'key': 'foo'})
        self.assertIsNone(result)
    
    def test_snapshot_and_restore(self):
        """Test creating a snapshot and restoring from it."""
        sm1 = DictStateMachine()
        
        sm1.apply({'action': 'set', 'key': 'foo', 'value': 'bar'})
        sm1.apply({'action': 'set', 'key': 'baz', 'value': 'qux'})
        
        snapshot = sm1.snapshot()
        
        sm2 = DictStateMachine()
        sm2.restore(snapshot)
        
        self.assertEqual(sm2.apply({'action': 'get', 'key': 'foo'}), 'bar')
        self.assertEqual(sm2.apply({'action': 'get', 'key': 'baz'}), 'qux')


class TestRaftNode(unittest.TestCase):
    """Test the RaftNode class."""
    
    def setUp(self):
        logging.disable(logging.CRITICAL)
        
        self.temp_dir = tempfile.mkdtemp()
        
        self.cluster_config = {
            'node1': {'address': 'localhost', 'port': 8001},
            'node2': {'address': 'localhost', 'port': 8002},
            'node3': {'address': 'localhost', 'port': 8003}
        }
        
        self.state_machine = DictStateMachine()
        
        self.node = RaftNode(
            node_id='node1',
            cluster_config=self.cluster_config,
            state_machine=self.state_machine,
            storage_dir=self.temp_dir,
            election_timeout_min=150,
            election_timeout_max=300,
            heartbeat_interval=50,
            test_mode=True  # Enable test mode to skip creating asyncio tasks
        )
        
        self.node.send_append_entries_callback = MagicMock(return_value=asyncio.Future())
        self.node.send_append_entries_callback.return_value.set_result({'success': True})
        
        self.node.send_request_vote_callback = MagicMock(return_value=asyncio.Future())
        self.node.send_request_vote_callback.return_value.set_result({'vote_granted': True})
    
    def tearDown(self):
        shutil.rmtree(self.temp_dir)
        
        logging.disable(logging.NOTSET)
    
    def test_initial_state(self):
        """Test the initial state of a Raft node."""
        self.assertEqual(self.node.node_id, 'node1')
        self.assertEqual(self.node.cluster_config, self.cluster_config)
        self.assertEqual(self.node.state_machine, self.state_machine)
        self.assertEqual(self.node.storage_dir, self.temp_dir)
        
        self.assertEqual(self.node.current_term, 0)
        self.assertIsNone(self.node.voted_for)
        self.assertEqual(self.node.state, NodeState.FOLLOWER)
        self.assertEqual(self.node.commit_index, 0)
        self.assertEqual(self.node.last_applied, 0)
    
    @patch('asyncio.create_task')
    async def test_election_timeout(self, mock_create_task):
        """Test that election timeout triggers an election."""
        mock_future = asyncio.Future()
        mock_create_task.return_value = mock_future
        
        self.node._reset_election_timer()
        
        args, kwargs = mock_create_task.call_args
        election_timeout_coro = args[0]
        
        with patch.object(self.node, '_start_election') as mock_start_election:
            mock_start_election.return_value = asyncio.Future()
            mock_start_election.return_value.set_result(None)
            
            await election_timeout_coro
            
            mock_start_election.assert_called_once()
    
    async def test_handle_append_entries(self):
        """Test handling AppendEntries RPCs."""
        args = {
            'term': 1,
            'leader_id': 'node2',
            'prev_log_index': 0,
            'prev_log_term': 0,
            'entries': [],
            'leader_commit': 0
        }
        
        response = await self.node.handle_append_entries(args)
        
        self.assertEqual(response['term'], 1)
        self.assertTrue(response['success'])
        
        self.assertEqual(self.node.current_term, 1)
        self.assertEqual(self.node.state, NodeState.FOLLOWER)
    
    async def test_handle_request_vote(self):
        """Test handling RequestVote RPCs."""
        args = {
            'term': 1,
            'candidate_id': 'node2',
            'last_log_index': 0,
            'last_log_term': 0
        }
        
        response = await self.node.handle_request_vote(args)
        
        self.assertEqual(response['term'], 1)
        self.assertTrue(response['vote_granted'])
        
        self.assertEqual(self.node.current_term, 1)
        self.assertEqual(self.node.voted_for, 'node2')


class TestRaftCounter(unittest.IsolatedAsyncioTestCase):
    """Test the RaftCounter class."""
    
    def setUp(self):
        self.mock_client = MagicMock(spec=RaftClient)
        self.counter = RaftCounter(self.mock_client, 'counter')
    
    async def test_increment(self):
        """Test incrementing the counter."""
        response = {'success': True, 'result': 42}
        
        async_mock = AsyncMock(return_value=response)
        self.mock_client.write = async_mock
        
        result = await self.counter.increment()
        
        self.mock_client.write.assert_called_once_with({
            'action': 'set',
            'key': 'counter',
            'value': {
                'operation': 'increment',
                'amount': 1
            }
        })
        
        self.assertEqual(result, 42)
    
    async def test_get(self):
        """Test getting the counter value."""
        response = {'success': True, 'result': 42}
        
        async_mock = AsyncMock(return_value=response)
        self.mock_client.read = async_mock
        
        result = await self.counter.get()
        
        self.mock_client.read.assert_called_once_with({
            'action': 'get',
            'key': 'counter'
        })
        
        self.assertEqual(result, 42)


if __name__ == '__main__':
    unittest.main()
