"""Percentage of equity sizing."""
from math import floor

class PercentSizer:
    """
    Sizes positions based on a percentage of available capital.
    """
    
    def __init__(self, percent: float = 0.10):
        """
        Initialize the sizer.
        
        Args:
            percent: Fraction of capital to risk per trade (default 0.10 for 10%)
        """
        self.percent = percent
        
    def calculate_quantity(self, price: float, capital: float, strength: float = 1.0, **kwargs) -> int:
        """
        Calculate position size.
        
        Args:
            price: Current price of the asset.
            capital: Available capital.
            strength: Signal strength (0.0-1.0).
            
        Returns:
            Reviewable quantity of shares.
        """
        if price <= 0:
            return 0
            
        target_allocation = capital * self.percent * strength
        quantity = floor(target_allocation / price)
        
        return quantity
