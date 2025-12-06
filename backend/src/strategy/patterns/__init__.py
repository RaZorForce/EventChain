"""
Pattern detection strategies.
"""
from .double_top import doubleTop
from .double_bottom import doubleBottom
from .triple_top import tripleTop
from .triple_bottom import tripleBottom
from .head_and_shoulders import headAndShoulders
from .head_and_shoulders_inverse import headAndShouldersInverse

__all__ = [
    'doubleTop',
    'doubleBottom',
    'tripleTop',
    'tripleBottom',
    'headAndShoulders',
    'headAndShouldersInverse',
]
