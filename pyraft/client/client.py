from typing import Dict, Any, Optional, List, Callable, Union
import asyncio
import logging
import random
import time


class RaftClient:
    """
    Client for interacting with a Raft cluster.
    
    This client provides methods to send commands to a Raft cluster and read from
    the state machine with strong read-after-write guarantees.
    """
    
    def __init__(
        self,
        cluster_config: Dict[str, Dict[str, Any]],
        timeout: float = 5.0,
        retry_interval: float = 0.5,
        max_retries: int = 5
    ):
        """
        Initialize a new Raft client.
        
        Args:
            cluster_config: Dictionary mapping node IDs to their configuration.
            timeout: Timeout for requests in seconds.
            retry_interval: Interval between retries in seconds.
            max_retries: Maximum number of retries for a request.
        """
        self.cluster_config = cluster_config
        self.timeout = timeout
        self.retry_interval = retry_interval
        self.max_retries = max_retries
        
        self.leader_id = None
        self.logger = logging.getLogger("raft.client")
        
        self.last_write_index = 0
    
    async def write(self, command: Dict[str, Any]) -> Dict[str, Any]:
        """
        Send a write command to the Raft cluster.
        
        This method will retry the request if the leader changes or if there are
        temporary failures.
        
        Args:
            command: The command to send.
            
        Returns:
            The response from the leader.
            
        Raises:
            TimeoutError: If the request times out after max_retries.
        """
        for retry in range(self.max_retries):
            try:
                if self.leader_id:
                    response = await self._send_to_node(self.leader_id, command, is_write=True)
                    
                    if response.get('success', False):
                        if 'index' in response:
                            self.last_write_index = response.get('index')
                            self.logger.debug(f"Updated last_write_index to {self.last_write_index}")
                        return response
                    
                    if response.get('error') == 'not_leader':
                        self.leader_id = response.get('leader_id')
                        self.logger.info(f"Redirected to new leader: {self.leader_id}")
                
                if not self.leader_id:
                    await self._find_leader()
                
                if not self.leader_id:
                    node_ids = list(self.cluster_config.keys())
                    random.shuffle(node_ids)
                    
                    for node_id in node_ids:
                        response = await self._send_to_node(node_id, command, is_write=True)
                        
                        if response.get('success', False):
                            self.leader_id = node_id
                            if 'index' in response:
                                self.last_write_index = response.get('index')
                                self.logger.debug(f"Updated last_write_index to {self.last_write_index}")
                            return response
                        
                        if response.get('error') == 'not_leader' and response.get('leader_id'):
                            self.leader_id = response.get('leader_id')
                            self.logger.info(f"Redirected to new leader: {self.leader_id}")
                            break
                
                await asyncio.sleep(self.retry_interval)
            except Exception as e:
                self.logger.warning(f"Error sending command (retry {retry+1}/{self.max_retries}): {e}")
                await asyncio.sleep(self.retry_interval)
        
        raise TimeoutError("Failed to send command after max retries")
    
    async def read(self, command: Dict[str, Any], linearizable: bool = True) -> Dict[str, Any]:
        """
        Send a read command to the Raft cluster.
        
        This method provides strong read-after-write guarantees by ensuring that
        the read is processed by the leader after all writes that this client has
        performed have been committed.
        
        Args:
            command: The read command to send.
            linearizable: Whether to use linearizable reads (stronger consistency but slower).
            
        Returns:
            The response from the leader.
            
        Raises:
            TimeoutError: If the request times out after max_retries.
        """
        for retry in range(self.max_retries):
            try:
                if self.leader_id:
                    if linearizable:
                        read_command = {
                            **command,
                            'linearizable': True,
                            'min_index': self.last_write_index
                        }
                    else:
                        read_command = command
                    
                    response = await self._send_to_node(self.leader_id, read_command, is_write=False)
                    
                    if response.get('success', False):
                        return response
                    
                    if response.get('error') == 'not_leader':
                        self.leader_id = response.get('leader_id')
                
                if not self.leader_id:
                    await self._find_leader()
                
                if not self.leader_id:
                    node_ids = list(self.cluster_config.keys())
                    random.shuffle(node_ids)
                    
                    for node_id in node_ids:
                        response = await self._send_to_node(node_id, command, is_write=False)
                        
                        if response.get('success', False):
                            self.leader_id = node_id
                            return response
                        
                        if response.get('error') == 'not_leader' and response.get('leader_id'):
                            self.leader_id = response.get('leader_id')
                            break
                
                await asyncio.sleep(self.retry_interval)
            except Exception as e:
                self.logger.warning(f"Error sending read command (retry {retry+1}/{self.max_retries}): {e}")
                await asyncio.sleep(self.retry_interval)
        
        raise TimeoutError("Failed to send read command after max retries")
    
    async def _send_to_node(self, node_id: str, command: Dict[str, Any], is_write: bool) -> Dict[str, Any]:
        """
        Send a command to a specific node.
        
        Args:
            node_id: The ID of the node to send to.
            command: The command to send.
            is_write: Whether this is a write command.
            
        Returns:
            The response from the node.
        """
        if node_id not in self.cluster_config:
            self.logger.warning(f"Unknown node ID: {node_id}")
            return {'error': 'unknown_node'}
            
        node_config = self.cluster_config[node_id]
        address = node_config.get('address', 'localhost')
        port = node_config.get('port', 0)
        
        self.logger.info(f"Sending {'write' if is_write else 'read'} request to {node_id} at {address}:{port}")
        
        try:
            reader, writer = await asyncio.open_connection(address, port)
            
            import msgpack
            message = {
                'type': 'client_request',
                'payload': {
                    'command': command,
                    'is_write': is_write
                }
            }
            
            self.logger.debug(f"Sending message to {node_id}: {message}")
            packed_message = msgpack.packb(message, use_bin_type=True)
            self.logger.debug(f"Packed message size: {len(packed_message)} bytes")
            
            writer.write(packed_message)
            await writer.drain()
            
            self.logger.debug(f"Message sent to {node_id}, waiting for response")
            
            try:
                data = await asyncio.wait_for(reader.read(4096), timeout=self.timeout)
                if not data:
                    self.logger.warning(f"Received empty data from {node_id}")
                    response = {'error': 'empty_response'}
                else:
                    self.logger.debug(f"Received data from {node_id}, size: {len(data)} bytes")
                    try:
                        unpacked = msgpack.unpackb(data, raw=False)
                        response = unpacked.get('response', {})
                        self.logger.debug(f"Unpacked response from {node_id}: {response}")
                    except Exception as unpack_error:
                        self.logger.error(f"Error unpacking response from {node_id}: {unpack_error}")
                        response = {'error': 'invalid_response_format'}
            except asyncio.TimeoutError:
                self.logger.warning(f"Timeout waiting for response from {node_id}")
                response = {'error': 'timeout'}
            
            writer.close()
            await writer.wait_closed()
            
            return response
        except ConnectionRefusedError:
            self.logger.error(f"Connection refused by {node_id} at {address}:{port}")
            return {'error': 'connection_refused'}
        except Exception as e:
            self.logger.error(f"Error sending to {node_id}: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            return {'error': f'connection_error: {str(e)}'}
    
    async def _find_leader(self) -> Optional[str]:
        """
        Try to find the current leader by querying all nodes.
        
        Returns:
            The ID of the leader if found, None otherwise.
        """
        node_ids = list(self.cluster_config.keys())
        random.shuffle(node_ids)
        
        for node_id in node_ids:
            try:
                node_config = self.cluster_config[node_id]
                address = node_config.get('address', 'localhost')
                port = node_config.get('port', 0)
                
                self.logger.info(f"Sending find_leader request to {node_id} at {address}:{port}")
                
                reader, writer = await asyncio.open_connection(address, port)
                
                import msgpack
                message = {
                    'type': 'find_leader',
                    'payload': {}
                }
                
                writer.write(msgpack.packb(message, use_bin_type=True))
                await writer.drain()
                
                data = await asyncio.wait_for(reader.read(4096), timeout=self.timeout)
                if not data:
                    self.logger.warning(f"Received empty data from {node_id}")
                    continue
                
                response = msgpack.unpackb(data, raw=False).get('response', {})
                self.logger.debug(f"Find leader response from {node_id}: {response}")
                
                writer.close()
                await writer.wait_closed()
                
                if response.get('leader_id'):
                    self.leader_id = response.get('leader_id')
                    self.logger.info(f"Found leader: {self.leader_id}")
                    return self.leader_id
            except Exception as e:
                self.logger.error(f"Error finding leader from {node_id}: {e}")
                continue
        
        for node_id in node_ids:
            try:
                response = await self._send_to_node(node_id, {'action': 'get', 'key': 'counter'}, is_write=False)
                
                if response.get('success', False):
                    self.leader_id = node_id
                    self.logger.info(f"Found leader through successful read: {self.leader_id}")
                    return self.leader_id
                
                if response.get('leader_id'):
                    self.leader_id = response.get('leader_id')
                    self.logger.info(f"Found leader through redirect: {self.leader_id}")
                    return self.leader_id
            except Exception as e:
                self.logger.error(f"Error with fallback leader check from {node_id}: {e}")
                continue
        
        return None


class RaftCounter:
    """
    A distributed counter using the Raft consensus protocol.
    
    This class provides a simple counter that commits increments to a Raft cluster
    and provides strong read-after-write guarantees.
    """
    
    def __init__(self, client: RaftClient, counter_key: str = 'counter'):
        """
        Initialize a new Raft counter.
        
        Args:
            client: The Raft client to use.
            counter_key: The key to use for the counter in the state machine.
        """
        self.client = client
        self.counter_key = counter_key
    
    async def increment(self, amount: int = 1) -> int:
        """
        Increment the counter by the specified amount.
        
        Args:
            amount: The amount to increment by.
            
        Returns:
            The new value of the counter.
            
        Raises:
            TimeoutError: If the request times out.
        """
        command = {
            'action': 'set',
            'key': self.counter_key,
            'value': {
                'operation': 'increment',
                'amount': amount
            }
        }
        
        response = await self.client.write(command)
        
        if not response.get('success', False):
            raise RuntimeError(f"Failed to increment counter: {response.get('error')}")
            
        return response.get('result', 0)
    
    async def get(self) -> int:
        """
        Get the current value of the counter.
        
        Returns:
            The current value of the counter.
            
        Raises:
            TimeoutError: If the request times out.
        """
        command = {
            'action': 'get',
            'key': self.counter_key
        }
        
        response = await self.client.read(command)
        
        if not response.get('success', False):
            raise RuntimeError(f"Failed to get counter value: {response.get('error')}")
            
        result = response.get('result')
        if result is None:
            return 0
            
        return result
