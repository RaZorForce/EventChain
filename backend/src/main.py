# -*- coding: utf-8 -*-

import os
import queue

from src.config import SYMBOLS, CSV_DIR, START_DATE, INITIAL_CAPITAL, STRATEGY_NAME, STRATEGY_REGISTRY
from src.engine import TradingEngine
from src.bars import HistoricCSVDataHandler
from src.portfolio import NaivePortfolio
from src.broker import SimulatedExecutionHandler


if __name__ == "__main__":
    # Resolve csv_dir relative to backend/
    backend_root = os.path.dirname(os.path.dirname(__file__))
    csv_dir = os.path.join(backend_root, CSV_DIR)

    # Create event queue
    events = queue.Queue()

    # Initialize components
    bars = HistoricCSVDataHandler(events, csv_dir, SYMBOLS)

    # Get strategy class from registry
    StrategyClass = STRATEGY_REGISTRY[STRATEGY_NAME]
    strategy = StrategyClass(bars, events)
    print(f"[main] Using strategy: {strategy.name}")

    portfolio = NaivePortfolio(bars, events, START_DATE, INITIAL_CAPITAL)
    broker = SimulatedExecutionHandler(events)

    # Run engine
    engine = TradingEngine(bars, strategy, portfolio, broker)
    engine.run()
    engine.print_summary()
