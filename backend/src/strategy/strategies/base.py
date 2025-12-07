# -*- coding: utf-8 -*-
"""
Base class for general algorithmic strategies (non-chart pattern specific).
"""
from queue import Queue

from src.data_handler import DataHandler
from src.strategy.base import Strategy

class Strategies(Strategy):
    """
    Base class for general trading strategies.
    
    This class can be expanded to include common utilities for:
    - Indicator calculation wrappers
    - General risk management
    - Execution logic common to algorithmic strategies
    """
    
    def __init__(self, bars: DataHandler, events: Queue):
        self.bars = bars
        self.events = events
        self.symbol_list = bars.symbol_list
