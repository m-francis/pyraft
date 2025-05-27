from abc import ABC, abstractmethod
from typing import Dict, Any, Callable, Awaitable, Optional, List
import asyncio
import logging
import time
import random

from pyraft.utils.network_metrics import NetworkMetrics


class Transport(ABC):
    """
    Abstract base class for network transports used by Raft nodes.
    
    A transport is responsible for sending and receiving messages between Raft nodes.
    Implementations must provide methods to send RPCs and register handlers for
    incoming RPCs.
    """
    
    @abstractmethod
    async def send_append_entries(self, target_id: str, args: Dict[str, Any]) -> Dict[str, Any]:
        """
        Send an AppendEntries RPC to a target node.
        
        Args:
            target_id: The ID of the target node.
            args: The AppendEntries arguments.
            
        Returns:
            The AppendEntries response.
        """
        pass
    
    @abstractmethod
    async def send_request_vote(self, target_id: str, args: Dict[str, Any]) -> Dict[str, Any]:
        """
        Send a RequestVote RPC to a target node.
        
        Args:
            target_id: The ID of the target node.
            args: The RequestVote arguments.
            
        Returns:
            The RequestVote response.
        """
        pass
    
    @abstractmethod
    async def start(self) -> None:
        """
        Start the transport.
        """
        pass
    
    @abstractmethod
    async def stop(self) -> None:
        """
        Stop the transport.
        """
        pass


class TCPTransport(Transport):
    """
    TCP-based implementation of the Transport interface.
    
    This transport uses TCP sockets for communication between Raft nodes.
    """
    
    def __init__(
        self,
        node_id: str,
        address: str,
        port: int,
        cluster_config: Dict[str, Dict[str, Any]],
        append_entries_handler: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]],
        request_vote_handler: Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]],
        client_request_handler: Optional[Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]]] = None,
        read_request_handler: Optional[Callable[[Dict[str, Any]], Awaitable[Dict[str, Any]]]] = None,
    ):
        """
        Initialize a new TCP transport.
        
        Args:
            node_id: The ID of the local node.
            address: The address to bind to.
            port: The port to bind to.
            cluster_config: Dictionary mapping node IDs to their configuration.
            append_entries_handler: Handler for AppendEntries RPCs.
            request_vote_handler: Handler for RequestVote RPCs.
            client_request_handler: Handler for client write requests.
            read_request_handler: Handler for client read requests.
        """
        self.node_id = node_id
        self.address = address
        self.port = port
        self.cluster_config = cluster_config
        self.append_entries_handler = append_entries_handler
        self.request_vote_handler = request_vote_handler
        self.client_request_handler = client_request_handler
        self.read_request_handler = read_request_handler
        
        self.network_metrics = NetworkMetrics()
        
        self.server = None
        self.logger = logging.getLogger(f"raft.transport.{node_id}")
    
    async def start(self) -> None:
        """
        Start the TCP server.
        """
        self.server = await asyncio.start_server(
            self._handle_connection,
            self.address,
            self.port
        )
        
        self.logger.info(f"TCP transport started on {self.address}:{self.port}")
    
    async def stop(self) -> None:
        """
        Stop the TCP server.
        """
        if self.server:
            self.server.close()
            await self.server.wait_closed()
            self.logger.info("TCP transport stopped")
    
    async def _handle_connection(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """
        Handle an incoming connection.
        
        Args:
            reader: The stream reader.
            writer: The stream writer.
        """
        peer_info = writer.get_extra_info('peername')
        self.logger.info(f"Received connection from {peer_info}")
        
        try:
            data = await reader.read(4096)
            if not data:
                self.logger.warning(f"Received empty data from {peer_info}")
                return
                
            import msgpack
            try:
                message = msgpack.unpackb(data, raw=False)
                
                message_type = message.get('type')
                payload = message.get('payload', {})
                
                self.logger.info(f"Received {message_type} RPC from {peer_info} with payload: {payload}")
                
                response = None
                if message_type == 'append_entries':
                    self.logger.debug(f"Processing append_entries from {peer_info}")
                    response = await self.append_entries_handler(payload)
                elif message_type == 'request_vote':
                    self.logger.debug(f"Processing request_vote from {peer_info}")
                    response = await self.request_vote_handler(payload)
                elif message_type == 'client_request':
                    self.logger.info(f"Received client_request from {peer_info}: {payload}")
                    if not self.client_request_handler:
                        self.logger.error(f"No client_request_handler registered for {peer_info}")
                        response = {'error': 'client_request_handler_not_registered'}
                    else:
                        is_write = payload.get('is_write', True)
                        command = payload.get('command', {})
                        
                        try:
                            if is_write:
                                self.logger.info(f"Handling client write request from {peer_info}: {command}")
                                response = await self.client_request_handler(command)
                                self.logger.info(f"Write response: {response}")
                            elif self.read_request_handler:
                                self.logger.info(f"Handling client read request from {peer_info}: {command}")
                                response = await self.read_request_handler(command)
                                self.logger.info(f"Read response: {response}")
                            else:
                                self.logger.warning(f"Received read request from {peer_info} but no read_request_handler registered")
                                response = {'error': 'read_handler_not_registered'}
                        except Exception as e:
                            self.logger.error(f"Error handling client request: {e}")
                            import traceback
                            self.logger.error(traceback.format_exc())
                            response = {'error': f'request_handler_error: {str(e)}'}
                elif message_type == 'find_leader':
                    self.logger.info(f"Received find_leader request from {peer_info}")
                    try:
                        if self.client_request_handler:
                            leader_check = {'action': 'find_leader', 'direct_check': True}
                            leader_response = await self.client_request_handler(leader_check)
                            self.logger.info(f"Leader check response: {leader_response}")
                            
                            if leader_response.get('is_leader', False):
                                response = {'leader_id': self.node_id}
                                self.logger.info(f"This node is the leader, reporting self: {self.node_id}")
                            else:
                                response = {'leader_id': leader_response.get('leader_id')}
                        else:
                            # Fallback if no client_request_handler
                            response = {'leader_id': None}
                            self.logger.warning("No client_request_handler to check leader status")
                        
                        if response.get('leader_id'):
                            self.logger.info(f"Reporting leader as {response['leader_id']}")
                        else:
                            self.logger.warning("No leader found to report")
                    except Exception as e:
                        self.logger.error(f"Error in find_leader handling: {e}")
                        import traceback
                        self.logger.error(traceback.format_exc())
                        response = {'error': f'find_leader_error: {str(e)}'}
                else:
                    self.logger.warning(f"Unknown message type from {peer_info}: {message_type}")
                    response = {'error': 'unknown_message_type'}
                
                self.logger.info(f"Sending response to {peer_info}: {response}")
                writer.write(msgpack.packb({'response': response}, use_bin_type=True))
                await writer.drain()
            except msgpack.exceptions.UnpackException as e:
                self.logger.error(f"Error unpacking message from {peer_info}: {e}, data: {data[:100]}")
                writer.write(msgpack.packb({'response': {'error': 'invalid_message_format'}}, use_bin_type=True))
                await writer.drain()
        except Exception as e:
            self.logger.error(f"Error handling connection from {peer_info}: {e}")
            import traceback
            self.logger.error(traceback.format_exc())
            try:
                import msgpack
                writer.write(msgpack.packb({'response': {'error': 'server_error'}}, use_bin_type=True))
                await writer.drain()
            except Exception as inner_e:
                self.logger.error(f"Error sending error response to {peer_info}: {inner_e}")
        finally:
            try:
                writer.close()
                await writer.wait_closed()
                self.logger.debug(f"Connection closed with {peer_info}")
            except Exception as e:
                self.logger.error(f"Error closing connection with {peer_info}: {e}")

    
    async def send_append_entries(self, target_id: str, args: Dict[str, Any]) -> Dict[str, Any]:
        """
        Send an AppendEntries RPC to a target node.
        
        Args:
            target_id: The ID of the target node.
            args: The AppendEntries arguments.
            
        Returns:
            The AppendEntries response.
        """
        return await self._send_rpc(target_id, 'append_entries', args)
    
    async def send_request_vote(self, target_id: str, args: Dict[str, Any]) -> Dict[str, Any]:
        """
        Send a RequestVote RPC to a target node.
        
        Args:
            target_id: The ID of the target node.
            args: The RequestVote arguments.
            
        Returns:
            The RequestVote response.
        """
        return await self._send_rpc(target_id, 'request_vote', args)
    
    async def _send_rpc(self, target_id: str, rpc_type: str, args: Dict[str, Any]) -> Dict[str, Any]:
        """
        Send an RPC to a target node with adaptive timeout and metrics tracking.
        
        Args:
            target_id: The ID of the target node.
            rpc_type: The type of RPC to send.
            args: The RPC arguments.
            
        Returns:
            The RPC response.
        """
        if target_id not in self.cluster_config:
            self.logger.warning(f"Unknown target node: {target_id}")
            return {'error': 'unknown_target'}
            
        target_config = self.cluster_config[target_id]
        target_address = target_config.get('address', 'localhost')
        target_port = target_config.get('port', 0)
        
        self.logger.debug(f"Sending {rpc_type} RPC to {target_id} at {target_address}:{target_port}")
        
        max_retries = 3
        base_retry_delay = 0.1  # seconds
        base_timeout = 1.0  # seconds
        
        adaptive_timeout = self.network_metrics.get_adaptive_timeout(base_timeout, target_id)
        self.logger.debug(f"Using adaptive timeout of {adaptive_timeout:.2f}s for {target_id}")
        
        for retry in range(max_retries):
            start_time = time.time()
            try:
                timeout = adaptive_timeout * (1.5 ** retry)
                retry_delay = base_retry_delay * (2 ** retry)
                
                self.logger.debug(f"Attempt {retry+1}/{max_retries} with timeout {timeout:.2f}s")
                
                reader, writer = await asyncio.open_connection(target_address, target_port)
                
                import msgpack
                message = {
                    'type': rpc_type,
                    'payload': args
                }
                
                writer.write(msgpack.packb(message, use_bin_type=True))
                await writer.drain()
                
                data = await asyncio.wait_for(reader.read(4096), timeout=timeout)
                if not data:
                    self.logger.warning(f"Received empty response from {target_id}")
                    writer.close()
                    await writer.wait_closed()
                    
                    self.network_metrics.record_failure(target_id)
                    
                    if retry < max_retries - 1:
                        await asyncio.sleep(retry_delay)
                        continue
                    return {'error': 'empty_response'}
                
                response = msgpack.unpackb(data, raw=False).get('response', {})
                
                writer.close()
                await writer.wait_closed()
                
                rtt = time.time() - start_time
                self.network_metrics.record_rtt(target_id, rtt)
                self.network_metrics.record_success(target_id)
                
                self.logger.debug(f"Received response from {target_id} in {rtt:.3f}s: {response}")
                return response
                
            except asyncio.TimeoutError:
                self.logger.warning(f"Timeout sending RPC to {target_id} (retry {retry+1}/{max_retries}, timeout: {timeout:.2f}s)")
                
                self.network_metrics.record_failure(target_id)
                
                if retry < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                    continue
                return {'error': 'timeout'}
                
            except ConnectionRefusedError:
                self.logger.warning(f"Connection refused by {target_id} at {target_address}:{target_port} (retry {retry+1}/{max_retries})")
                
                self.network_metrics.record_failure(target_id)
                
                if retry < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                    continue
                return {'error': 'connection_refused'}
                
            except Exception as e:
                self.logger.error(f"Error sending RPC to {target_id}: {e} (retry {retry+1}/{max_retries})")
                
                self.network_metrics.record_failure(target_id)
                
                if retry < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                    continue
                return {'error': 'connection_error'}
        
        return {'error': 'max_retries_exceeded'}
