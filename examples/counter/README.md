# Raft Counter Example

This example demonstrates a distributed counter application using the PyRaft consensus protocol library. The counter is replicated across three Raft nodes and provides strong read-after-write guarantees.

## Overview

The example consists of:

1. A counter state machine that maintains counter values
2. Three Raft server nodes that form a consensus cluster
3. A client that interacts with the cluster to increment and read counter values

## Running the Example

### 1. Start the Raft Servers

Start three server instances in separate terminals:

```bash
# Terminal 1
python server.py --node-id node1 --config cluster_config.json --storage-dir /tmp/raft-node1

# Terminal 2
python server.py --node-id node2 --config cluster_config.json --storage-dir /tmp/raft-node2

# Terminal 3
python server.py --node-id node3 --config cluster_config.json --storage-dir /tmp/raft-node3
```

### 2. Run the Client

In a fourth terminal, run the client to interact with the cluster:

```bash
python client.py --config cluster_config.json --num-operations 10
```

The client will:
1. Connect to the Raft cluster
2. Increment the counter multiple times
3. Read the counter value after each increment to verify read-after-write guarantees
4. Log the operations and results

## Testing Read-After-Write Guarantees

The client verifies read-after-write guarantees by:

1. Incrementing the counter and recording the new value
2. Immediately reading the counter value
3. Verifying that the read value matches the expected value after the increment

## Testing Fault Tolerance

To test fault tolerance:

1. Start all three servers
2. Run the client and observe successful operations
3. Stop one server (e.g., Ctrl+C in its terminal)
4. Continue running client operations - they should still succeed as long as a majority (2 out of 3) servers are running
5. Restart the stopped server - it should catch up with the current state

## Examining Logs

Each server and the client generate log files that can be examined to understand the Raft protocol in action:

- `node1.log`, `node2.log`, `node3.log`: Server logs showing leader election, log replication, etc.
- `client.log`: Client operations and verification of read-after-write guarantees
