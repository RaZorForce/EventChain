"""
Signal event - emitted by Strategy when trading opportunity detected.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any

from .base import Event


@dataclass(frozen=True)
class SignalEvent(Event):
    """
    Emitted by Strategy when a trading opportunity is detected.
    
    Contains signal information but not position sizing -
    that's the Portfolio's responsibility.
    """
    type: str = 'SIGNAL'
    strategy_id: str = ''
    symbol: str = ''
    signal_type: str = ''  # 'LONG', 'SHORT', 'EXIT'
    strength: float = 1.0  # Signal confidence (0.0-1.0)
    timestamp: datetime = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate and set defaults."""
        if self.timestamp is None:
            object.__setattr__(self, 'timestamp', datetime.utcnow())
        if self.signal_type not in ['LONG', 'SHORT', 'EXIT']:
            raise ValueError(f"Invalid signal_type: {self.signal_type}")
