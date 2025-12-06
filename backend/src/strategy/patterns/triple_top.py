# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from queue import Queue
from typing import Tuple
from scipy.signal import find_peaks
from icecream import ic
import mplfinance as mpf
import matplotlib.pyplot as plt

from src.bars import DataHandler
from src.events import SignalEvent

from ..base import Strategy


class tripleTop(Strategy):

    def __init__(self, bars: DataHandler, events: Queue) -> None:
        self.name = "Triple Top"
        self.datapoints = 5
        self.bias = "Short"
        self.bars: DataHandler = bars
        self.symbol_list: list = bars.symbol_list
        self.latest_symbol_data: dict = bars.latest_symbol_data
        self.events: Queue = events

        self.highs = {sym: [] for sym in bars.symbol_list}
        self.lows = {sym: [] for sym in bars.symbol_list}
        self.date = {sym: [] for sym in bars.symbol_list}

        self.pattern_data: dict = {sym: False for sym in bars.symbol_list}
        for s in self.symbol_list:
            self.pattern_data[s] = pd.DataFrame({'is_detected': [False],'is_confirmed': [False],'is_bought': [False],\
                                                 'top1_date': [np.nan], 'neck1_date': [np.nan], 'top2_date': [np.nan], 'neck2_date': [np.nan], 'top3_date': [np.nan],\
                                                 'top1_price': [np.nan], 'neck1_price': [np.nan], 'top2_price': [np.nan], 'neck2_price': [np.nan], 'top3_price': [np.nan],\
                                                 'confirmation_date': [pd.NaT], 'signal': [np.nan], 'time_for_confirmation': [np.nan]}, index=[0])

        self.found: dict = {sym: False for sym in bars.symbol_list}
        self.detected: dict = {sym: False for sym in bars.symbol_list}
        self.confirmed = {sym: False for sym in bars.symbol_list}
        self.bought: dict = {sym: False for sym in bars.symbol_list}
        self.pattern_state = {sym: "SCANNING" for sym in bars.symbol_list}

    def calculate_signals(self, event: Queue) -> None:
        """
        Process each MarketEvent and check for triple top pattern.
        Emits SHORT signal when pattern is confirmed.
        """
        if event.type == 'MARKET':
            for s in self.symbol_list:
                bars = self.bars.get_latest_bars(s, 1)
                minima, maxima = self.get_min_max(self.latest_symbol_data[s])

                if len(minima) !=0 and len(maxima)!= 0:
                    if str(self.latest_symbol_data[s].index[-1]) == "2024-02-08 00:00:00":
                        pass
                        self.plot_min_max(self.latest_symbol_data[s], minima, maxima)

                if self.pattern_state[s] == "SCANNING":
                    pattern_dates = self.pattern_scanner(minima, maxima)
                    #collect the pattern price points
                    price_data = self.get_PriceData(self.latest_symbol_data[s], pattern_dates)
                    
                    if len(price_data) != 0:
                        # Initialize status columns for the new candidates
                        price_data['is_confirmed'] = False
                        price_data['is_bought'] = False
                        price_data['signal'] = np.nan
                        price_data['confirmation_date'] = pd.NaT
                        
                        # Replace the state with the found patterns
                        self.pattern_data[s] = price_data
                        
                    if self.pattern_data[s]['is_detected'].any():
                        self.pattern_state[s] = "CONFIRMING"

                elif self.pattern_state[s] == "CONFIRMING":
                    # Store the information for confirmation with the rest of the pattern data
                    self.pattern_data[s] = self.get_ConfDate(self.latest_symbol_data[s], self.pattern_data[s])

                    if not self.pattern_data[s].empty and self.pattern_data[s]['is_confirmed'].any():
                        self.pattern_state[s] = "BUYING"

                elif self.pattern_state[s] == "BUYING":
                    if not self.bought[s]:
                        bars = self.bars.get_latest_bars(s, N=1)
                        signal = SignalEvent(s, bars.index[0], 'SHORT')
                        self.events.put(signal)
                        self.bought[s] = True
                        print(f"[tripleTop] Generated SHORT signal for {s}")

    def plot_min_max(self, data: pd.DataFrame, minima: float, maxima: float):
        # List of data points that fall under the minima category
        min_points = [minima.loc[k] if k in minima.index else np.nan for k in data.index]
        max_points = [maxima.loc[k] if k in maxima.index else np.nan for k in data.index]

        # Additional plots for marking the support and resistance levels
        apd = [mpf.make_addplot(min_points, type='scatter', color="green",marker='^', markersize=400),
               mpf.make_addplot(max_points, type='scatter', color="red", marker='v', markersize=400)]

        # Plot the OHLC data along with the lines passing through the nearest support and resistance levels
        mpf.plot(data, type='candle', style='classic', addplot=apd, title=str(data.index[-1]),figsize=(15, 7), block=True)
        plt.close()

    def get_min_max(self, df: pd.DataFrame, window: int = 10) -> Tuple[pd.DataFrame, pd.DataFrame]:
        peaks_idx_high, _ = find_peaks(df['High'], height=None, prominence=0.5, distance=10)
        valleys_idx_low, _ = find_peaks(-df['Low'], height=None, prominence=0.5, distance=10)
        return df.iloc[valleys_idx_low].Low, df.iloc[peaks_idx_high].High

    def pattern_scanner(self, minima: pd.Series, maxima: pd.Series, frequency: str = 'daily') -> list:
        """
        Scan for triple top pattern: A-B-C-D-E where A,C,E are tops and B,D are necklines.
        """
        patterns = []
        minima_series = pd.Series(minima)
        maxima_series = pd.Series(maxima)
        min_max = pd.concat([minima_series, maxima_series]).sort_index()

        for i in range(self.datapoints, len(min_max) + 1):
            window = min_max.iloc[i - self.datapoints:i]
            window_size = (window.index[-1] - window.index[0]).days

            if window_size > 100:
                continue

            A, B, C, D, E = [window.iloc[j] for j in range(0, len(window))]

            # cond_1: B, D are in minima (necklines/support)
            cond_1 = all(x in minima.values for x in [B, D])

            # cond_2: A, C, E are in maxima (tops)
            cond_2 = all(x in maxima.values for x in [A, C, E])

            # cond_3: Necklines are below the tops
            cond_3 = (B < A) and (B < C) and (D < E) and (D < C)

            # cond_4: Tops within 15% of each other, necklines within 15% (relaxed from 1.5%)
            cond_4 = (abs(A - C) <= np.mean([A, C]) * 0.15) and \
                     (abs(C - E) <= np.mean([C, E]) * 0.15) and \
                     (abs(B - D) <= np.mean([B, D]) * 0.15)

            if cond_1 and cond_2 and cond_3 and cond_4:
                patterns.append([window.index[j] for j in range(0, len(window))])

        return patterns

    def get_PriceData(self, data: pd.DataFrame, pattern_list: list) -> pd.DataFrame:
        pattern_data = pd.DataFrame(pattern_list, columns=['top1_date', 'neck1_date', 'top2_date', 'neck2_date', 'top3_date'])

        pattern_data['top1_price'] = data.loc[pattern_data.top1_date, 'High'].values
        pattern_data['neck1_price'] = data.loc[pattern_data.neck1_date, 'Low'].values
        pattern_data['top2_price'] = data.loc[pattern_data.top2_date, 'High'].values
        pattern_data['neck2_price'] = data.loc[pattern_data.neck2_date, 'Low'].values
        pattern_data['top3_price'] = data.loc[pattern_data.top3_date, 'High'].values
        pattern_data['is_detected'] = True

        return pattern_data

    def get_ConfDate(self, data: pd.DataFrame, pattern_data: pd.DataFrame):
        """
        Check if price closed below neckline (confirmation of breakdown).
        """
        if len(pattern_data) != 0:
            if 'confirmation_date' not in pattern_data.columns:
                pattern_data['confirmation_date'] = pd.NaT
                pattern_data['confirmation_date'] = pattern_data['confirmation_date'].astype('object')

            for x in range(0, len(pattern_data)):
                data_after_top3 = data.loc[pattern_data.at[x, 'top3_date']:]['Close']

                try:
                    # Confirm when price closes BELOW neck2
                    pattern_data.at[x, 'confirmation_date'] = data_after_top3[
                        data_after_top3 < pattern_data.at[x, 'neck2_price']
                    ].index[0]

                    pattern_data[['confirmation_date']] = pattern_data[['confirmation_date']].apply(pd.to_datetime, format='%Y-%m-%d')

                    pattern_data.at[x, 'time_for_confirmation'] = (
                        pattern_data.at[x, 'confirmation_date'] - pattern_data.at[x, 'top3_date']
                    ).days

                except IndexError:
                    pattern_data.at[x, 'confirmation_date'] = np.nan
                except:
                    pattern_data.at[x, 'confirmation_date'] = np.nan

            pattern_data['signal'] = -1
            
            # Set is_confirmed based on whether a confirmation date was found
            pattern_data['is_confirmed'] = pd.notna(pattern_data['confirmation_date'])
            
            num_confirmed = pattern_data['is_confirmed'].sum()
            if num_confirmed > 0:
                print(f"[tripleTop] Pattern confirmed! Found {num_confirmed} triple top pattern(s)")
            
            pattern_data.reset_index(drop=True, inplace=True)

        return pattern_data

    def risk_Manager(self, pattern_data: pd.DataFrame):
        if len(pattern_data) != 0:
            for x in range(0, len(pattern_data)):
                # Stop-loss 1% above the third top
                pattern_data.at[x, 'stoploss'] = round(pattern_data.at[x, 'top3_price'] * 1.01, 2)

                # Calculate pattern height
                pattern_data.at[x, 'top_length'] = round(
                    pattern_data.at[x, 'top3_price'] - pattern_data.at[x, 'neck2_price'], 2
                )

                # Target: pattern height projected below neckline
                pattern_data.at[x, 'target'] = round(
                    pattern_data.at[x, 'neck2_price'] - pattern_data.at[x, 'top_length'], 2
                )
