from abc import ABC, abstractmethod
from typing import Any, Dict, Optional


class StateMachine(ABC):
    """
    Abstract base class for state machines that can be used with the Raft consensus protocol.
    
    A state machine represents the application logic that processes commands once they
    have been committed by the Raft consensus algorithm. Implementations must provide
    methods to apply commands, create snapshots, and restore from snapshots.
    """
    
    @abstractmethod
    def apply(self, command: Dict[str, Any]) -> Any:
        """
        Apply a command to the state machine.
        
        Args:
            command: The command to apply, typically a dictionary with an action and parameters.
            
        Returns:
            The result of applying the command.
        """
        pass
    
    @abstractmethod
    def snapshot(self) -> bytes:
        """
        Create a snapshot of the current state.
        
        Returns:
            A binary representation of the current state.
        """
        pass
    
    @abstractmethod
    def restore(self, snapshot: bytes) -> None:
        """
        Restore the state machine from a snapshot.
        
        Args:
            snapshot: A binary representation of a state, as returned by snapshot().
        """
        pass


class DictStateMachine(StateMachine):
    """
    A simple dictionary-based state machine implementation.
    
    This state machine stores key-value pairs and supports get, set, and delete operations.
    It is useful for simple applications and as a reference implementation.
    """
    
    def __init__(self):
        self._data = {}
    
    def apply(self, command: Dict[str, Any]) -> Any:
        """
        Apply a command to the dictionary state machine.
        
        Supported commands:
        - {'action': 'get', 'key': str}
        - {'action': 'set', 'key': str, 'value': Any}
        - {'action': 'delete', 'key': str}
        
        Args:
            command: The command to apply.
            
        Returns:
            For 'get', returns the value or None if the key doesn't exist.
            For 'set', returns the new value.
            For 'delete', returns the deleted value or None if the key didn't exist.
        """
        action = command.get('action')
        key = command.get('key')
        
        if action == 'get':
            return self._data.get(key)
        elif action == 'set':
            value = command.get('value')
            self._data[key] = value
            return value
        elif action == 'delete':
            return self._data.pop(key, None)
        else:
            raise ValueError(f"Unknown action: {action}")
    
    def snapshot(self) -> bytes:
        """
        Create a snapshot of the current state.
        
        Returns:
            A msgpack-encoded representation of the current state.
        """
        import msgpack
        return msgpack.packb(self._data, use_bin_type=True)
    
    def restore(self, snapshot: bytes) -> None:
        """
        Restore the state machine from a snapshot.
        
        Args:
            snapshot: A msgpack-encoded representation of a state.
        """
        import msgpack
        self._data = msgpack.unpackb(snapshot, raw=False)
