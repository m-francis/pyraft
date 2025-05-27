#!/usr/bin/env python3
import asyncio
import argparse
import logging
import os
import json
import sys
import time
import traceback

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from pyraft.client.client import RaftClient, RaftCounter

class CounterWithStaleReads(RaftCounter):
    """
    Extended RaftCounter with support for stale reads during partitions.
    """
    
    async def get_with_stale_option(self, allow_stale: bool = False) -> int:
        """
        Get the current value of the counter with optional stale read support.
        
        Args:
            allow_stale: Whether to allow stale reads during partitions.
        
        Returns:
            The current value of the counter.
            
        Raises:
            TimeoutError: If the request times out.
        """
        command = {
            'action': 'get',
            'key': self.counter_key
        }
        
        response = await self.client.read(command, allow_stale=allow_stale)
        
        if not response.get('success', False):
            raise RuntimeError(f"Failed to get counter value: {response.get('error')}")
        
        if response.get('stale'):
            logger = logging.getLogger("raft.client")
            logger.warning(f"Stale read detected: {response.get('warning')}")
            logger.warning(f"Partition state: {response.get('partition_state')}")
        
        result = response.get('result')
        if result is None:
            return 0
            
        return result


async def run_client(config_path, counter_key='counter', num_operations=5, delay=1.0, test_stale_reads=False):
    """
    Run a client that interacts with a Raft cluster.
    
    Args:
        config_path: Path to the cluster configuration file.
        counter_key: The key to use for the counter in the state machine.
        num_operations: Number of increment operations to perform.
        delay: Delay between operations in seconds.
        test_stale_reads: Whether to test stale reads during partitions.
    """
    logging.basicConfig(
        level=logging.DEBUG,  # Changed to DEBUG for more detailed logs
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler("client.log")
        ]
    )
    
    logger = logging.getLogger("raft.client")
    logger.info("Starting Raft client")
    
    with open(config_path, 'r') as f:
        cluster_config = json.load(f)
    
    logger.info(f"Loaded cluster configuration: {cluster_config}")
    
    logger.info("Waiting for servers to be ready...")
    await asyncio.sleep(3)
    
    client = RaftClient(
        cluster_config=cluster_config,
        timeout=10.0,       # Increased timeout
        retry_interval=1.0, # Increased retry interval
        max_retries=10      # Increased max retries
    )
    
    counter = CounterWithStaleReads(client, counter_key)
    
    try:
        logger.info("Attempting to find leader...")
        leader_id = await client._find_leader()
        if leader_id:
            logger.info(f"Found leader: {leader_id}")
        else:
            logger.warning("No leader found, will try operations anyway")
            
        if test_stale_reads:
            logger.info("Testing stale reads during partitions...")
            try:
                normal_value = await counter.get()
                logger.info(f"Normal read value: {normal_value}")
                
                stale_value = await counter.get_with_stale_option(allow_stale=True)
                logger.info(f"Stale-allowed read value: {stale_value}")
                
                if normal_value == stale_value:
                    logger.info("Stale read test: values match (no partition detected)")
                else:
                    logger.warning(f"Stale read test: values differ! Normal: {normal_value}, Stale: {stale_value}")
            except Exception as e:
                logger.error(f"Error during stale read test: {e}")
                logger.error(traceback.format_exc())
        
        initial_value = None
        for retry in range(5):
            try:
                logger.info(f"Attempting to get initial counter value (attempt {retry+1}/5)...")
                initial_value = await counter.get()
                logger.info(f"Initial counter value: {initial_value}")
                break
            except Exception as e:
                logger.error(f"Failed to get initial value (attempt {retry+1}/5): {e}")
                if retry < 4:  # Don't sleep on the last retry
                    await asyncio.sleep(2)
        
        if initial_value is None:
            logger.warning("Could not get initial value, assuming 0")
            initial_value = 0
        
        success_count = 0
        for i in range(num_operations):
            logger.info(f"Starting operation {i+1}/{num_operations}")
            
            try:
                logger.debug(f"Sending increment command...")
                new_value = await counter.increment()
                logger.info(f"Incremented counter to: {new_value}")
                
                logger.debug(f"Sending read command...")
                read_value = await counter.get()
                logger.info(f"Read counter value: {read_value}")
                
                if read_value != new_value:
                    logger.error(f"Read-after-write guarantee violated! Expected {new_value}, got {read_value}")
                else:
                    logger.info("Read-after-write guarantee verified")
                    success_count += 1
                
                await asyncio.sleep(delay)
                
            except TimeoutError as e:
                logger.error(f"Timeout error: {e}")
                leader_id = await client._find_leader()
                if leader_id:
                    logger.info(f"Found new leader: {leader_id}")
                await asyncio.sleep(2)
            except Exception as e:
                logger.error(f"Error during operation {i+1}: {e}")
                logger.error(traceback.format_exc())
                await asyncio.sleep(2)
        
        try:
            final_value = await counter.get()
            logger.info(f"Final counter value: {final_value}")
            logger.info(f"Expected final value: {initial_value + success_count}")
            
            if final_value == initial_value + success_count:
                logger.info("Counter operations completed successfully")
            else:
                logger.warning(f"Counter operations partially succeeded: {success_count}/{num_operations} operations successful")
        except Exception as e:
            logger.error(f"Failed to get final value: {e}")
    
    except Exception as e:
        logger.error(f"Error: {e}")
        logger.error(traceback.format_exc())


def main():
    """
    Parse command line arguments and start the client.
    """
    parser = argparse.ArgumentParser(description='Run a client that interacts with a Raft cluster')
    parser.add_argument('--config', required=True, help='Path to cluster configuration file')
    parser.add_argument('--counter-key', default='counter', help='Key to use for the counter')
    parser.add_argument('--num-operations', type=int, default=5, help='Number of increment operations to perform')
    parser.add_argument('--delay', type=float, default=1.0, help='Delay between operations in seconds')
    parser.add_argument('--test-stale-reads', action='store_true', help='Test stale reads during partitions')
    
    args = parser.parse_args()
    
    asyncio.run(run_client(
        args.config, 
        args.counter_key, 
        args.num_operations, 
        args.delay,
        args.test_stale_reads
    ))


if __name__ == '__main__':
    main()
