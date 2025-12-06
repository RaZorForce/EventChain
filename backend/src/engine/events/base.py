"""
Base Event class for the event-driven trading system.

All events inherit from this base class and are immutable.
"""
from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class Event:
    """
    Base class for all events in the trading system.
    
    Events are immutable (frozen) to prevent bugs from modification
    and to enable event replay for debugging.
    """
    version: str = "1.0"  # For backward compatibility
    
    def __post_init__(self):
        """Validate event after initialization."""
        if not hasattr(self, 'type'):
            raise AttributeError(f"{self.__class__.__name__} must define 'type' attribute")
