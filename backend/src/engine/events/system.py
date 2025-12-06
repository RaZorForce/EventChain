"""
System event - for engine control and system-level events.
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, Any

from .base import Event


@dataclass(frozen=True)
class SystemEvent(Event):
    """
    Emitted by Engine for system-level control and events.
    
    Subtypes:
    - TIMER: Scheduled events (EOD, rebalancing, heartbeat)
    - SHUTDOWN: Graceful system shutdown
    - PAUSE/RESUME: Pause/resume trading
    - ERROR: System-level errors
    """
    type: str = 'SYSTEM'
    event_subtype: str = ''  # 'TIMER', 'SHUTDOWN', 'PAUSE', 'RESUME', 'ERROR'
    severity: str = 'INFO'  # 'INFO', 'WARNING', 'CRITICAL'
    message: str = ''
    timestamp: datetime = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def __post_init__(self):
        """Validate and set defaults."""
        if self.timestamp is None:
            object.__setattr__(self, 'timestamp', datetime.utcnow())
        if self.event_subtype not in ['TIMER', 'SHUTDOWN', 'PAUSE', 'RESUME', 'ERROR']:
            raise ValueError(f"Invalid event_subtype: {self.event_subtype}")
        if self.severity not in ['INFO', 'WARNING', 'CRITICAL']:
            raise ValueError(f"Invalid severity: {self.severity}")
