from typing import Dict, Any, Optional
import msgpack
import logging

from pyraft.core.state_machine import StateMachine


class CounterStateMachine(StateMachine):
    """
    A state machine implementation for a simple counter.
    
    This state machine maintains counters identified by keys and supports
    increment, decrement, and get operations.
    """
    
    def __init__(self):
        """
        Initialize a new counter state machine.
        """
        self._counters = {}
        self.logger = logging.getLogger("raft.counter.state_machine")
    
    def apply(self, command: Dict[str, Any]) -> Any:
        """
        Apply a command to the counter state machine.
        
        Supported commands:
        - {'action': 'get', 'key': str}
        - {'action': 'set', 'key': str, 'value': {'operation': 'increment', 'amount': int}}
        - {'action': 'set', 'key': str, 'value': {'operation': 'decrement', 'amount': int}}
        - {'action': 'set', 'key': str, 'value': int}
        - {'action': 'delete', 'key': str}
        
        Args:
            command: The command to apply.
            
        Returns:
            For 'get', returns the counter value or 0 if the key doesn't exist.
            For 'set' with increment/decrement, returns the new counter value.
            For 'set' with direct value, returns the new counter value.
            For 'delete', returns the deleted counter value or 0 if the key didn't exist.
        """
        action = command.get('action')
        key = command.get('key')
        
        if action == 'get':
            return self._counters.get(key, 0)
        
        elif action == 'set':
            value = command.get('value')
            
            if isinstance(value, dict) and 'operation' in value:
                operation = value.get('operation')
                amount = value.get('amount', 1)
                
                if operation == 'increment':
                    current = self._counters.get(key, 0)
                    new_value = current + amount
                    self._counters[key] = new_value
                    self.logger.debug(f"Incremented counter {key} by {amount} to {new_value}")
                    return new_value
                
                elif operation == 'decrement':
                    current = self._counters.get(key, 0)
                    new_value = current - amount
                    self._counters[key] = new_value
                    self.logger.debug(f"Decremented counter {key} by {amount} to {new_value}")
                    return new_value
            
            else:
                self._counters[key] = value
                self.logger.debug(f"Set counter {key} to {value}")
                return value
        
        elif action == 'delete':
            return self._counters.pop(key, 0)
            
        elif action == 'find_leader':
            self.logger.debug("Received find_leader action in state machine")
            return {'message': 'find_leader should be handled by node'}
        
        elif action == 'no_op':
            self.logger.debug("Received no-op action in state machine")
            return None
        
        else:
            raise ValueError(f"Unknown action: {action}")
    
    def snapshot(self) -> bytes:
        """
        Create a snapshot of the current state.
        
        Returns:
            A msgpack-encoded representation of the current counters.
        """
        return msgpack.packb(self._counters, use_bin_type=True)
    
    def restore(self, snapshot: bytes) -> None:
        """
        Restore the state machine from a snapshot.
        
        Args:
            snapshot: A msgpack-encoded representation of counters.
        """
        self._counters = msgpack.unpackb(snapshot, raw=False)
        self.logger.info(f"Restored {len(self._counters)} counters from snapshot")
