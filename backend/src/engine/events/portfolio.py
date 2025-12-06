"""
Portfolio state event - combines position, risk, and account information.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict

from .base import Event


@dataclass(frozen=True)
class PortfolioStateEvent(Event):
    """
    Emitted by Portfolio when state changes significantly.
    
    Combines position, risk, and account information into a single event.
    Strategies use this to know "can I trade?" and current exposure.
    """
    type: str = 'PORTFOLIO_STATE'
    
    # Position info
    positions: Dict[str, int] = field(default_factory=dict)  # {symbol: quantity}
    
    # Account info
    total_equity: float = 0.0
    cash: float = 0.0
    unrealized_pnl: float = 0.0
    realized_pnl: float = 0.0
    
    # Risk info
    max_drawdown: float = 0.0
    risk_status: str = 'NORMAL'  # 'NORMAL', 'WARNING', 'CRITICAL', 'HALTED'
    risk_message: str = ''
    
    timestamp: datetime = None
    
    def __post_init__(self):
        """Validate and set defaults."""
        if self.timestamp is None:
            object.__setattr__(self, 'timestamp', datetime.utcnow())
        if self.risk_status not in ['NORMAL', 'WARNING', 'CRITICAL', 'HALTED']:
            raise ValueError(f"Invalid risk_status: {self.risk_status}")
