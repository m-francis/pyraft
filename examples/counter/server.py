#!/usr/bin/env python3
import asyncio
import argparse
import logging
import os
import json
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from pyraft.core.node import RaftNode
from pyraft.network.transport import TCPTransport
from counter_state_machine import CounterStateMachine


async def run_server(node_id, config_path, storage_dir=None):
    """
    Run a Raft server with the counter state machine.
    
    Args:
        node_id: The ID of this node.
        config_path: Path to the cluster configuration file.
        storage_dir: Directory to store persistent state.
    """
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(f"{node_id}.log")
        ]
    )
    
    logger = logging.getLogger("raft.server")
    logger.info(f"Starting Raft server with node ID: {node_id}")
    
    with open(config_path, 'r') as f:
        cluster_config = json.load(f)
    
    if storage_dir and not os.path.exists(storage_dir):
        os.makedirs(storage_dir)
    
    state_machine = CounterStateMachine()
    
    node = RaftNode(
        node_id=node_id,
        cluster_config=cluster_config,
        state_machine=state_machine,
        storage_dir=storage_dir
    )
    
    node_config = cluster_config.get(node_id, {})
    address = node_config.get('address', 'localhost')
    port = node_config.get('port', 8000)
    
    transport = TCPTransport(
        node_id=node_id,
        address=address,
        port=port,
        cluster_config=cluster_config,
        append_entries_handler=node.handle_append_entries,
        request_vote_handler=node.handle_request_vote,
        client_request_handler=node.handle_client_request,
        read_request_handler=node.handle_read_request
    )
    
    node.send_append_entries_callback = transport.send_append_entries
    node.send_request_vote_callback = transport.send_request_vote
    
    logging.getLogger("raft").setLevel(logging.DEBUG)
    logging.getLogger("raft.transport").setLevel(logging.DEBUG)
    
    await transport.start()
    
    logger.info(f"Server started on {address}:{port}")
    
    try:
        while True:
            await asyncio.sleep(1)
    except asyncio.CancelledError:
        logger.info("Server shutting down")
    finally:
        await transport.stop()


def main():
    """
    Parse command line arguments and start the server.
    """
    parser = argparse.ArgumentParser(description='Run a Raft server with the counter state machine')
    parser.add_argument('--node-id', required=True, help='ID of this node')
    parser.add_argument('--config', required=True, help='Path to cluster configuration file')
    parser.add_argument('--storage-dir', help='Directory to store persistent state')
    
    args = parser.parse_args()
    
    asyncio.run(run_server(args.node_id, args.config, args.storage_dir))


if __name__ == '__main__':
    main()
