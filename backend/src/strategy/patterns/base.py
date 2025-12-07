# -*- coding: utf-8 -*-
"""
Base class for all chart pattern-based strategies.
"""
from abc import abstractmethod
from typing import Tuple, Dict, List
from queue import Queue
from datetime import datetime

import numpy as np
import pandas as pd
import mplfinance as mpf
import matplotlib.pyplot as plt
from scipy.signal import find_peaks

from src.data_handler import DataHandler
from src.engine.events import SignalEvent
from src.strategy.base import Strategy

class Patterns(Strategy):
    """
    Base class for pattern recognition strategies (e.g., Double Top, Head & Shoulders).
    
    Encapsulates common logic for:
    - Peak/Valley detection (get_min_max)
    - Visualization (plot_min_max)
    - Risk Management calculations (risk_Manager)
    - State management (SCANNING -> CONFIRMING -> BUYING)
    """

    def __init__(self, bars: DataHandler, events: Queue):
        self.bars = bars
        self.events = events
        self.symbol_list = bars.symbol_list
        self.latest_symbol_data = bars.latest_symbol_data
        
        # Strategy defaults - should be overridden by subclasses
        self.name = "Pattern"
        self.datapoints = 3
        self.bias = "Long"  # "Long" or "Short"

        # Initialization
        self.highs = {sym: [] for sym in self.symbol_list}
        self.lows = {sym: [] for sym in self.symbol_list}
        self.date = {sym: [] for sym in self.symbol_list}
        
        self.pattern_data: Dict[str, pd.DataFrame] = {sym: pd.DataFrame() for sym in self.symbol_list}
        self.found = {sym: False for sym in self.symbol_list}
        self.detected = {sym: False for sym in self.symbol_list}
        self.confirmed = {sym: False for sym in self.symbol_list}
        self.bought = {sym: False for sym in self.symbol_list}
        self.pattern_state = {sym: "SCANNING" for sym in self.symbol_list}

        # Initialize pattern_data structure for each symbol
        for s in self.symbol_list:
            self.reset_pattern_data(s)

    def reset_pattern_data(self, symbol: str):
        """Initializes or resets the pattern data DataFrame for a symbol."""
        # This structure is common but fields might vary slightly. 
        # Subclasses can extend this if needed, but this base covers the core flow.
        self.pattern_data[symbol] = pd.DataFrame({
            'is_detected': [False],
            'is_confirmed': [False],
            'is_bought': [False],
            'confirmation_date': [pd.NaT], 
            'signal': [np.nan], 
            'time_for_confirmation': [np.nan]
        }, index=[0])

    @abstractmethod
    def pattern_scanner(self, minima: pd.Series, maxima: pd.Series, frequency: str = 'daily') -> list:
        """
        Scans for the specific pattern. Must be implemented by subclass.
        Returns a list of window indices where pattern is found.
        """
        raise NotImplementedError("Should implement pattern_scanner()")

    @abstractmethod
    def get_PriceData(self, data: pd.DataFrame, pattern_list: list) -> pd.DataFrame:
        """
        Extracts price data for the found pattern. Must be implemented by subclass.
        """
        raise NotImplementedError("Should implement get_PriceData()")

    @abstractmethod
    def get_ConfDate(self, data: pd.DataFrame, pattern_data: pd.DataFrame) -> pd.DataFrame:
        """
        Checks for confirmation conditions (breakout). Must be implemented by subclass.
        """
        raise NotImplementedError("Should implement get_ConfDate()")

    @abstractmethod
    def risk_Manager(self, pattern_data: pd.DataFrame):
        """
        Calculates stop-loss and targets. Must be implemented by subclass.
        """
        raise NotImplementedError("Should implement risk_Manager()")

    def calculate_signals(self, event):
        """
        Main execution loop for pattern strategies.
        """
        if event.type == 'MARKET':
            for s in self.symbol_list:
                # 1. Check if we already have a position to prevent duplicate buys
                # Note: This basic check prevents multiple signals for the same symbol
                if self.bought[s]:
                    continue

                self.bars.get_latest_bars(s, 1)
                
                # 2. Get Min/Max
                minima, maxima = self.get_min_max(self.latest_symbol_data[s])
                
                if len(minima) == 0 or len(maxima) == 0:
                    continue

                # Optional: Visualization hooks
                if str(self.latest_symbol_data[s].index[-1]) == "2024-02-08 00:00:00":
                    self.plot_min_max(self.latest_symbol_data[s], minima, maxima)

                # 3. State Machine
                if self.pattern_state[s] == "SCANNING":
                    pattern_dates = self.pattern_scanner(minima, maxima)
                    price_data = self.get_PriceData(self.latest_symbol_data[s], pattern_dates)
                    
                    if len(price_data) != 0:
                        # Initialize status columns
                        price_data['is_confirmed'] = False
                        price_data['is_bought'] = False
                        price_data['signal'] = np.nan
                        price_data['confirmation_date'] = pd.NaT
                        
                        self.pattern_data[s] = price_data
                        
                    if not self.pattern_data[s].empty and 'is_detected' in self.pattern_data[s] and self.pattern_data[s]['is_detected'].any():
                        self.pattern_state[s] = "CONFIRMING"

                elif self.pattern_state[s] == "CONFIRMING":
                    self.pattern_data[s] = self.get_ConfDate(self.latest_symbol_data[s], self.pattern_data[s])
                    
                    if not self.pattern_data[s].empty and self.pattern_data[s]['is_confirmed'].any():
                        self.risk_Manager(self.pattern_data[s])
                        self.pattern_state[s] = "BUYING"

                elif self.pattern_state[s] == "BUYING":
                    if not self.bought[s]:
                        bars = self.bars.get_latest_bars(s, N=1)
                        # Construct proper signal type
                        sig_type = 'LONG' if self.bias == 'Long' else 'SHORT'
                        
                        signal = SignalEvent(symbol=s, timestamp=bars.index[0], signal_type=sig_type)
                        self.events.put(signal)
                        self.bought[s] = True
                        print(f"[{self.name}] Generated {sig_type} signal for {s}")

    def get_min_max(self, df: pd.DataFrame, window: int = 10) -> Tuple[pd.Series, pd.Series]:
        """
        Finds local minima and maxima in the provided DataFrame.
        """
        peaks_idx_high, _ = find_peaks(df['High'], height=None, prominence=0.5, distance=10)
        valleys_idx_low, _ = find_peaks(-df['Low'], height=None, prominence=0.5, distance=10)
        return df.iloc[valleys_idx_low].Low, df.iloc[peaks_idx_high].High

    def plot_min_max(self, data: pd.DataFrame, minima: pd.Series, maxima: pd.Series):
        """
        Plots the asset price with marked minima and maxima.
        """
        min_points = [minima.loc[k] if k in minima.index else np.nan for k in data.index]
        max_points = [maxima.loc[k] if k in maxima.index else np.nan for k in data.index]

        apd = [mpf.make_addplot(min_points, type='scatter', color="green", marker='^', markersize=400),
               mpf.make_addplot(max_points, type='scatter', color="red", marker='v', markersize=400)]

        mpf.plot(data, type='candle', style='classic', addplot=apd, title=str(data.index[-1]), figsize=(15, 7), block=True)
        plt.close()
