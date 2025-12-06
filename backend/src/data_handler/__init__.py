"""
DataHandler island - handles market data feeds.

Provides both historical (CSV) and live data handling with a unified interface.
"""
from .base import DataHandler
from .csv import HistoricCSVDataHandler

__all__ = [
    'DataHandler',
    'HistoricCSVDataHandler',
]
