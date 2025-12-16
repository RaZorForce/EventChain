# -*- coding: utf-8 -*-

import os
import queue

from src.config import SYMBOLS, CSV_DIR, START_DATE, INITIAL_CAPITAL, STRATEGY_NAME, STRATEGY_REGISTRY
from src.engine import TradingEngine
from src.data_handler import HistoricCSVDataHandler
from src.portfolio import NaivePortfolio, BasicPortfolio
from src.portfolio import PercentSizer
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

    sizer = PercentSizer(0.10)
    portfolio = BasicPortfolio(bars, events, INITIAL_CAPITAL, sizer=sizer)
    broker = SimulatedExecutionHandler(events)

    # Run engine
    engine = TradingEngine(bars, strategy, portfolio, broker)
    engine.run()
    engine.print_summary()
