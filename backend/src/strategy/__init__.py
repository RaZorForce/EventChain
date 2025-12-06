"""
Strategy island - trading strategy implementations.
"""
from .base import Strategy
from .patterns import (
    doubleTop,
    doubleBottom,
    tripleTop,
    tripleBottom,
    headAndShoulders,
    headAndShouldersInverse,
)
from .strategies import BuyAndHoldStrategy

__all__ = [
    'Strategy',
    'doubleTop',
    'doubleBottom',
    'tripleTop',
    'tripleBottom',
    'headAndShoulders',
    'headAndShouldersInverse',
    'BuyAndHoldStrategy',
]
