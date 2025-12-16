"""
Fixed quantity position sizing.
"""
from math import floor


class FixedSizer:
    """
    Simple fixed quantity position sizer.
    """
    
    def __init__(self, default_quantity: int = 100):
        """
        Initialize fixed sizer.
        
        Args:
            default_quantity: Default number of shares per trade
        """
        self.default_quantity = default_quantity
    
    def calculate_quantity(self, signal_strength: float = 1.0, **kwargs) -> int:
        """
        Calculate position size.
        
        Args:
            signal_strength: Signal strength (0.0-1.0)
        
        Returns:
            Number of shares to trade
        """
        return floor(self.default_quantity * signal_strength)
