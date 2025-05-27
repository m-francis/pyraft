import logging
import json
import time
import os
from typing import Dict, Any, Optional
from enum import Enum


class PartitionAwareFormatter(logging.Formatter):
    """Custom formatter for partition-aware logging."""
    
    def format(self, record):
        """
        Format log records with partition context.
        
        Adds additional fields to log records:
        - node_id: The ID of the node
        - partition_state: Current partition state
        - leader_id: Current leader ID
        - term: Current term
        """
        message = super().format(record)
        
        context = {
            'timestamp': time.time(),
            'level': record.levelname,
            'logger': record.name,
            'message': record.getMessage(),
            'node_id': getattr(record, 'node_id', None),
            'partition_state': getattr(record, 'partition_state', None),
            'leader_id': getattr(record, 'leader_id', None),
            'term': getattr(record, 'term', None),
        }
        
        context = {k: v for k, v in context.items() if v is not None}
        
        for key, value in context.items():
            if isinstance(value, Enum):
                context[key] = value.value
        
        if hasattr(record, 'json_format') and record.json_format:
            return json.dumps(context)
        
        context_str = ' '.join([f"{k}={v}" for k, v in context.items() 
                               if k not in ('timestamp', 'level', 'logger', 'message')])
        
        if context_str:
            return f"{message} [{context_str}]"
        return message


def setup_partition_logging(node_id: str, log_dir: Optional[str] = None, 
                           log_level: int = logging.INFO, 
                           enable_json: bool = False):
    """
    Setup logging configuration for partition-aware logging.
    
    Args:
        node_id: The ID of the node.
        log_dir: Directory to store log files. If None, only console logging is used.
        log_level: Logging level (default: INFO).
        enable_json: Whether to enable JSON formatted logs (default: False).
    """
    if enable_json:
        formatter = logging.Formatter('%(message)s')
    else:
        formatter = PartitionAwareFormatter(
            '%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level)
    
    handlers = [console_handler]
    
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)
        log_file = os.path.join(log_dir, f"raft-{node_id}.log")
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(formatter)
        file_handler.setLevel(log_level)
        handlers.append(file_handler)
        
        if enable_json:
            json_log_file = os.path.join(log_dir, f"raft-{node_id}-json.log")
            json_handler = logging.FileHandler(json_log_file)
            json_formatter = PartitionAwareFormatter('%(message)s')
            json_handler.setFormatter(json_formatter)
            
            class JsonFilter(logging.Filter):
                def filter(self, record):
                    record.json_format = True
                    return True
                
            json_handler.addFilter(JsonFilter())
            handlers.append(json_handler)
    
    logging.basicConfig(
        level=log_level,
        handlers=handlers,
        force=True
    )
    
    old_factory = logging.getLogRecordFactory()
    
    def record_factory(*args, **kwargs):
        record = old_factory(*args, **kwargs)
        record.node_id = node_id
        return record
    
    logging.setLogRecordFactory(record_factory)
    
    logger = logging.getLogger("raft.logging")
    logger.info(f"Partition-aware logging initialized for node {node_id}")
    
    return logger


def add_partition_context(logger, partition_state=None, leader_id=None, term=None):
    """
    Add partition context to a logger.
    
    Args:
        logger: The logger to add context to.
        partition_state: Current partition state.
        leader_id: Current leader ID.
        term: Current term.
    
    Returns:
        A logger with partition context.
    """
    class PartitionContextFilter(logging.Filter):
        def filter(self, record):
            if partition_state is not None:
                record.partition_state = partition_state
            if leader_id is not None:
                record.leader_id = leader_id
            if term is not None:
                record.term = term
            return True
    
    for handler in logger.handlers:
        handler.addFilter(PartitionContextFilter())
    
    return logger
