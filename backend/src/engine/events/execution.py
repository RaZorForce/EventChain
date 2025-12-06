"""
Execution event - emitted by Broker when order execution status changes.
"""
from dataclasses import dataclass
from datetime import datetime

from .base import Event


@dataclass(frozen=True)
class ExecutionEvent(Event):
    """
    Emitted by Broker when order execution status changes.
    
    Replaces both FillEvent and OrderStatusEvent - handles all
    execution outcomes (partial fills, complete fills, rejections, etc.)
    """
    type: str = 'EXECUTION'
    order_id: str = ''
    status: str = ''  # 'PARTIAL', 'FILLED', 'REJECTED', 'CANCELLED'
    symbol: str = ''
    direction: str = ''  # 'BUY', 'SELL'
    filled_quantity: int = 0  # Cumulative filled
    remaining_quantity: int = 0
    avg_fill_price: float = 0.0
    commission: float = 0.0
    timestamp: datetime = None
    reason: str = ''  # For rejections/cancellations
    
    def __post_init__(self):
        """Validate and set defaults."""
        if self.timestamp is None:
            object.__setattr__(self, 'timestamp', datetime.utcnow())
        if self.status not in ['PARTIAL', 'FILLED', 'REJECTED', 'CANCELLED']:
            raise ValueError(f"Invalid status: {self.status}")
