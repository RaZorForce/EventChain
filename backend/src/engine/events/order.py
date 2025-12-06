"""
Order event - emitted by Portfolio to request trade execution.
"""
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from .base import Event


@dataclass(frozen=True)
class OrderEvent(Event):
    """
    Emitted by Portfolio to request trade execution.
    
    Contains all order details including type, quantity, and price limits.
    """
    type: str = 'ORDER'
    order_id: str = ''
    symbol: str = ''
    order_type: str = 'MKT'  # 'MKT', 'LMT', 'STP'
    direction: str = ''  # 'BUY', 'SELL'
    quantity: int = 0
    price: Optional[float] = None  # For limit orders
    stop_price: Optional[float] = None  # For stop orders
    time_in_force: str = 'DAY'  # 'DAY', 'GTC', 'IOC'
    timestamp: datetime = None
    
    def __post_init__(self):
        """Validate and set defaults."""
        if self.timestamp is None:
            object.__setattr__(self, 'timestamp', datetime.utcnow())
        if self.direction not in ['BUY', 'SELL']:
            print(f"Invalid direction isssss: {self.direction}")
            raise ValueError(f"Invalid direction: {self.direction}")
        if self.quantity <= 0:
            raise ValueError(f"Invalid quantity: {self.quantity}")
