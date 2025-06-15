"""
Constants and enumerations for the Raft consensus protocol.
"""

from enum import Enum, auto


class NodeState(Enum):
    """
    Represents the possible states of a Raft node.
    """
    FOLLOWER = auto()
    CANDIDATE = auto()
    LEADER = auto()
