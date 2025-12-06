"""
Broker island - order execution.
"""
from .base import ExecutionHandler
from .simulated import SimulatedExecutionHandler
# from .interactive_brokers import IBExecutionHandler  # Requires 'ib' package

__all__ = [
    'ExecutionHandler',
    'SimulatedExecutionHandler',
    # 'IBExecutionHandler',  # Commented out - requires optional dependency
]
