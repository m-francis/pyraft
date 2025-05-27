from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
import msgpack
import os
import json


@dataclass
class LogEntry:
    """
    Represents a single entry in the Raft log.
    
    Attributes:
        term: The term in which the entry was created.
        command: The command to be applied to the state machine.
        index: The index of this entry in the log (1-based).
    """
    term: int
    command: Dict[str, Any]
    index: int


class RaftLog:
    """
    Represents the replicated log in the Raft consensus algorithm.
    
    The log consists of entries that are replicated to all servers. Each entry
    contains a command to be executed by the state machine and the term when the
    entry was received by the leader.
    """
    
    def __init__(self, storage_dir: Optional[str] = None):
        """
        Initialize a new Raft log.
        
        Args:
            storage_dir: Directory to store log entries persistently. If None, log is in-memory only.
        """
        self._entries = []  # List of LogEntry objects
        self._storage_dir = storage_dir
        
        if storage_dir and not os.path.exists(storage_dir):
            os.makedirs(storage_dir)
            
        if storage_dir:
            self._load_from_disk()
    
    def append(self, term: int, command: Dict[str, Any]) -> LogEntry:
        """
        Append a new entry to the log.
        
        Args:
            term: The current term.
            command: The command to append.
            
        Returns:
            The newly created log entry.
        """
        index = len(self._entries) + 1
        entry = LogEntry(term=term, command=command, index=index)
        self._entries.append(entry)
        
        if self._storage_dir:
            self._persist_entry(entry)
            
        return entry
    
    def get_entry(self, index: int) -> Optional[LogEntry]:
        """
        Get the entry at the specified index.
        
        Args:
            index: The 1-based index of the entry to get.
            
        Returns:
            The log entry at the specified index, or None if the index is out of bounds.
        """
        if 1 <= index <= len(self._entries):
            return self._entries[index - 1]
        return None
    
    def get_entries_from(self, start_index: int) -> List[LogEntry]:
        """
        Get all entries starting from the specified index.
        
        Args:
            start_index: The 1-based index to start from.
            
        Returns:
            A list of log entries starting from the specified index.
        """
        if start_index <= 0:
            start_index = 1
            
        if start_index > len(self._entries):
            return []
            
        return self._entries[start_index - 1:]
    
    def get_last_log_term_and_index(self) -> Tuple[int, int]:
        """
        Get the term and index of the last entry in the log.
        
        Returns:
            A tuple of (term, index) for the last entry, or (0, 0) if the log is empty.
        """
        if not self._entries:
            return 0, 0
            
        last_entry = self._entries[-1]
        return last_entry.term, last_entry.index
    
    def delete_entries_from(self, start_index: int) -> None:
        """
        Delete all entries starting from the specified index.
        
        This is used when a follower needs to delete inconsistent entries.
        
        Args:
            start_index: The 1-based index to start deleting from.
        """
        if start_index <= len(self._entries):
            self._entries = self._entries[:start_index - 1]
            
            if self._storage_dir:
                self._truncate_persistent_log(start_index - 1)
    
    def get_size(self) -> int:
        """
        Get the number of entries in the log.
        
        Returns:
            The number of entries in the log.
        """
        return len(self._entries)
    
    def _persist_entry(self, entry: LogEntry) -> None:
        """
        Persist a log entry to disk.
        
        Args:
            entry: The log entry to persist.
        """
        if self._storage_dir is None:
            return
            
        entry_path = os.path.join(self._storage_dir, f"entry_{entry.index}.json")
        with open(entry_path, 'w') as f:
            json.dump({
                'term': entry.term,
                'command': entry.command,
                'index': entry.index
            }, f)
    
    def _load_from_disk(self) -> None:
        """
        Load log entries from disk.
        """
        if self._storage_dir is None or not os.path.exists(self._storage_dir):
            return
            
        entry_files = [f for f in os.listdir(self._storage_dir) if f.startswith("entry_") and f.endswith(".json")]
        
        entry_files.sort(key=lambda f: int(f.split('_')[1].split('.')[0]))
        
        for file_name in entry_files:
            if self._storage_dir is not None:
                file_path = os.path.join(self._storage_dir, file_name)
                with open(file_path, 'r') as f:
                    data = json.load(f)
                    entry = LogEntry(
                        term=data['term'],
                        command=data['command'],
                        index=data['index']
                    )
                    self._entries.append(entry)
    
    def _truncate_persistent_log(self, end_index: int) -> None:
        """
        Truncate the persistent log to the specified index.
        
        Args:
            end_index: The index to truncate to (inclusive).
        """
        if self._storage_dir is None:
            return
            
        for file_name in os.listdir(self._storage_dir):
            if file_name.startswith("entry_") and file_name.endswith(".json"):
                index = int(file_name.split('_')[1].split('.')[0])
                if index > end_index:
                    os.remove(os.path.join(self._storage_dir, file_name))
