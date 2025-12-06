"""
Strategy island - trading strategy implementations.
"""
from .base import Strategy
from .patterns import doubleTop, doubleBottom
from .strategies import BuyAndHoldStrategy

__all__ = [
    'Strategy',
    'doubleTop',
    'doubleBottom',
    'BuyAndHoldStrategy',
]
