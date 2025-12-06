# Public API for the trading system
from src.data_handler import DataHandler, HistoricCSVDataHandler
from src.engine import (
    TradingEngine,
    Event,
    MarketEvent,
    SignalEvent,
    OrderEvent,
    ExecutionEvent,
    PortfolioStateEvent,
    SystemEvent,
)
from src.strategy import Strategy, BuyAndHoldStrategy, doubleTop, doubleBottom
from src.portfolio import Portfolio, NaivePortfolio
from src.broker import ExecutionHandler, SimulatedExecutionHandler
from src.performance import create_sharpe_ratio, create_drawdowns

__all__ = [
    'DataHandler', 'HistoricCSVDataHandler',
    'TradingEngine',
    'Event', 'MarketEvent', 'SignalEvent', 'OrderEvent', 'ExecutionEvent',
    'PortfolioStateEvent', 'SystemEvent',
    'Strategy', 'BuyAndHoldStrategy', 'doubleTop', 'doubleBottom',
    'Portfolio', 'NaivePortfolio',
    'ExecutionHandler', 'SimulatedExecutionHandler',
    'create_sharpe_ratio', 'create_drawdowns',
]
