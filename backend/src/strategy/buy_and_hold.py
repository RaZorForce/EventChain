# -*- coding: utf-8 -*-

# strategy.py

from queue import Queue

from src.bars import DataHandler
from src.events import SignalEvent

from .base import Strategy


class BuyAndHoldStrategy(Strategy):
    """
    This is an extremely simple strategy that goes LONG all of the
    symbols as soon as a bar is received. It will never exit a position.

    It is primarily used as a testing mechanism for the Strategy class
    as well as a benchmark upon which to compare other strategies.
    """

    def __init__(self, bars: DataHandler, events: Queue) -> None:
        """
        Initialises the buy and hold strategy.

        Parameters:
        bars - The DataHandler object that provides bar information
        events - The Event Queue object.
        """
        self.bars: DataHandler = bars
        self.symbol_list: list = bars.symbol_list
        self.latest_symbol_data: dict = bars.latest_symbol_data
        self.events: Queue = events

        # Once buy & hold signal is given, these are set to True
        self.bought: dict = self._calculate_initial_bought()

    def _calculate_initial_bought(self) -> dict:
        """
        Adds keys to the bought dictionary for all symbols
        and sets them to False.
        """
        bought: dict = {}
        for s in self.symbol_list:
            bought[s] = False
        return bought

    def calculate_signals(self, event: Queue) -> None:
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
                bars: list = self.bars.get_latest_bars(s, N=2)
                if bars is not None and len(bars) > 0:
                    if self.bought[s] == False:
                        # (Symbol, Datetime, Type = LONG, SHORT or EXIT)
                        signal: SignalEvent = SignalEvent(s, bars.index[0], 'LONG')
                        self.events.put(signal)
                        self.bought[s] = True
