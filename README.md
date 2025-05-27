# PyRaft

A Python implementation of the Raft consensus protocol as described in the [Raft paper](https://raft.github.io/) and [Wikipedia](https://en.wikipedia.org/wiki/Raft_(algorithm)).

## Overview

PyRaft is a library that implements the Raft consensus protocol in Python. It provides a robust, fault-tolerant way to maintain a replicated state machine across a cluster of servers. The implementation includes:

- Leader election
- Log replication
- Safety properties
- Membership changes
- Strong read-after-write guarantees

## Installation

```bash
pip install pyraft
```

Or install from source:

```bash
git clone https://github.com/m-francis/pyraft.git
cd pyraft
pip install -e .
```

## Usage

### Basic Example

```python
from pyraft.core.node import RaftNode
from pyraft.core.state_machine import DictStateMachine
from pyraft.network.transport import TCPTransport

# Create a state machine
state_machine = DictStateMachine()

# Create a Raft node
node = RaftNode(
    node_id="node1",
    state_machine=state_machine,
    storage_dir="/tmp/raft-node1",
    cluster_config={
        "node1": {"address": "localhost", "port": 8001},
        "node2": {"address": "localhost", "port": 8002},
        "node3": {"address": "localhost", "port": 8003}
    }
)

# Start the node
await node.start()
```

### Counter Example

See the [counter example](examples/counter) for a complete example of a distributed counter using PyRaft.

## Features

- **Strong Consistency**: PyRaft provides strong consistency guarantees through the Raft consensus protocol.
- **Fault Tolerance**: The system continues to operate as long as a majority of servers are functioning.
- **Linearizable Reads**: Ensures that reads reflect all writes that have been acknowledged to clients.
- **Modular Design**: Easily integrate with your own state machines and network transports.

## License

MIT
