"""
Position tracking module.

Tracks open positions, quantities, and cost basis.
"""
import pandas as pd


class PositionTracker:
    """
    Tracks positions across all symbols.
    """
    
    def __init__(self, symbol_list: list):
        """
        Initialize position tracker.
        
        Args:
            symbol_list: List of symbols to track
        """
        self.symbol_list = symbol_list
        self.current_positions = {s: 0 for s in symbol_list}
        self.all_positions = []
    
    def update_position(self, symbol: str, quantity_change: int, timestamp):
        """
        Update position for a symbol.
        
        Args:
            symbol: Symbol to update
            quantity_change: Change in quantity (positive for buy, negative for sell)
            timestamp: Time of update
        """
        self.current_positions[symbol] += quantity_change
        
        # Record position snapshot
        position_snapshot = {s: self.current_positions[s] for s in self.symbol_list}
        position_snapshot['datetime'] = timestamp
        self.all_positions.append(position_snapshot)
    
    def get_position(self, symbol: str) -> int:
        """Get current position for a symbol."""
        return self.current_positions.get(symbol, 0)
    
    def get_all_positions(self) -> dict:
        """Get all current positions."""
        return self.current_positions.copy()
