"""
Market event - signals that new market data is available.
"""
from dataclasses import dataclass
from datetime import datetime

from .base import Event


@dataclass(frozen=True)
class MarketEvent(Event):
    """
    Emitted by DataHandler when new market data is available.
    
    Consumers (Strategy, Portfolio) pull data from DataHandler
    using get_latest_bars() method.
    """
    type: str = 'MARKET'
    timestamp: datetime = None
    
    def __post_init__(self):
        """Set timestamp if not provided."""
        if self.timestamp is None:
            object.__setattr__(self, 'timestamp', datetime.utcnow())
