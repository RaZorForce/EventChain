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
from src.strategy import (
    Strategy, 
    BuyAndHoldStrategy, 
    doubleTop, 
    doubleBottom,
    tripleTop,
    tripleBottom,
    headAndShoulders,
    headAndShouldersInverse,
)
from src.portfolio import Portfolio, NaivePortfolio
from src.broker import ExecutionHandler, SimulatedExecutionHandler
from src.portfolio.metrics import create_sharpe_ratio, create_drawdowns

__all__ = [
    'DataHandler', 'HistoricCSVDataHandler',
    'TradingEngine',
    'Event', 'MarketEvent', 'SignalEvent', 'OrderEvent', 'ExecutionEvent',
    'PortfolioStateEvent', 'SystemEvent',
    'Strategy', 'BuyAndHoldStrategy', 
    'doubleTop', 'doubleBottom', 'tripleTop', 'tripleBottom',
    'headAndShoulders', 'headAndShouldersInverse',
    'Portfolio', 'NaivePortfolio',
    'ExecutionHandler', 'SimulatedExecutionHandler',
    'create_sharpe_ratio', 'create_drawdowns',
]
