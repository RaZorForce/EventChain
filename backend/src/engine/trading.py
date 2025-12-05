# -*- coding: utf-8 -*-

import queue

from src.bars import DataHandler
from src.strategy import Strategy
from src.portfolio import Portfolio
from src.broker import ExecutionHandler


class TradingEngine:
    """
    Universal trading engine - works for both backtest and live trading.
    The mode is determined by the DataHandler passed in.
    """

    def __init__(self, bars: DataHandler, strategy: Strategy,
                 portfolio: Portfolio, broker: ExecutionHandler):
        self.bars = bars
        self.strategy = strategy
        self.portfolio = portfolio
        self.broker = broker
        self.events = bars.events

    def _process_event(self, event):
        """Handle a single event from the queue."""
        if event.type == 'MARKET':
            self.strategy.calculate_signals(event)
            self.portfolio.update_timeindex(event)
        elif event.type == 'SIGNAL':
            print(f"[SIGNAL] {event.signal_type} signal for {event.symbol} | Queue: {self.events.qsize()} remaining")
            self.portfolio.update_signal(event)
        elif event.type == 'ORDER':
            print(f"[ORDER]  {event.direction} {event.quantity} shares of {event.symbol} | Queue: {self.events.qsize()} remaining")
            self.broker.execute_order(event)
        elif event.type == 'FILL':
            print(f"[FILL]   {event.direction} {event.quantity} shares of {event.symbol} @ ${event.fill_cost:.2f} | Queue: {self.events.qsize()} remaining")
            self.portfolio.update_fill(event)

    def run(self):
        """Main event loop - universal for backtest and live."""
        while self.bars.continue_backtest:
            self.bars.update_bars()

            while True:
                try:
                    event = self.events.get(False)
                except queue.Empty:
                    break
                if event is not None:
                    self._process_event(event)
                    self.events.task_done()

    def get_results(self):
        """Generate and return performance results."""
        self.portfolio.create_equity_curve_dataframe()
        return self.portfolio.output_summary_stats()

    def print_summary(self):
        """Print backtest results to console."""
        print("\n" + "="*60)
        print(" "*20 + "BACKTEST RESULTS")
        print("="*60)

        stats = self.get_results()
        for stat in stats:
            print(stat)
