# -*- coding: utf-8 -*-
"""
Buy and Hold Strategy
"""
from queue import Queue

from src.data_handler import DataHandler
from src.engine.events import SignalEvent
from .base import Strategies


class BuyAndHoldStrategy(Strategies):
    """
    This is an extremely simple strategy that goes LONG all of the
    symbols as soon as a bar is received. It will never exit a position.

    It is primarily used as a testing mechanism for the Strategy class
    as well as a benchmark upon which to compare other strategies.
    """

    def __init__(self, bars: DataHandler, events: Queue):
        """
        Initialises the buy and hold strategy.

        Parameters:
        bars - The DataHandler object that provides bar information
        events - The Event Queue object
        """
        super().__init__(bars, events)
        self.name = "Buy & Hold"
        self.bought = self._calculate_initial_bought()

    def _calculate_initial_bought(self):
        """
        Adds keys to the bought dictionary for all symbols
        and sets them to 'False'.
        """
        bought = {}
        for s in self.symbol_list:
            bought[s] = False
        return bought

    def calculate_signals(self, event):
        """
        For "Buy and Hold" we generate a single signal per symbol
        and then no additional signals. This means we are
        constantly long the market from the date of strategy
        initialisation.

        Parameters
        event - A MarketEvent object.
        """
        if event.type == 'MARKET':
            for s in self.symbol_list:
                bars = self.bars.get_latest_bars(s, N=1)
                if bars is not None and bars != [] and len(bars) > 0:
                    if self.bought[s] == False:
                        # (Symbol, Datetime, Type = LONG, SHORT or EXIT)
                        signal = SignalEvent(symbol=s, timestamp=bars.index[0], signal_type='LONG')
                        self.events.put(signal)
                        self.bought[s] = True
