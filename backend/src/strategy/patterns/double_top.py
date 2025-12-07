# -*- coding: utf-8 -*-

# strategy.py

import numpy as np
import pandas as pd
from queue import Queue
from typing import Tuple
from scipy.signal import find_peaks
from icecream import ic
import mplfinance as mpf
import matplotlib.pyplot as plt

from src.data_handler import DataHandler
from src.engine.events import SignalEvent

from ..base import Strategy


class doubleTop(Strategy):

    def __init__(self, bars: DataHandler, events: Queue) -> None:
        self.name = "Double Top"
        self.datapoints = 3
        self.bias = "Short"
        self.bars: DataHandler = bars
        self.symbol_list: list = bars.symbol_list
        self.latest_symbol_data: dict = bars.latest_symbol_data
        self.events: Queue = events

        self.highs = {sym: [] for sym in bars.symbol_list}
        self.lows = {sym: [] for sym in bars.symbol_list}
        self.date = {sym: [] for sym in bars.symbol_list}

        self.pattern_data : dict = {sym: False for sym in bars.symbol_list}
        for s in self.symbol_list:
            self.pattern_data[s] = pd.DataFrame({'is_detected': [False],'is_confirmed': [False],'is_bought': [False],\
                                                 'top1_date': [np.nan], 'neck1_date': [np.nan], 'top2_date': [np.nan],\
                                                 'top1_price': [np.nan], 'neck1_price': [np.nan], 'top2_price': [np.nan],\
                                                 'confirmation_date': [pd.NaT], 'signal': [np.nan], 'time_for_confirmation': [np.nan]}, index=[0])

        # Once buy & hold signal is given, these are set to True
        self.found: dict = {sym: False for sym in bars.symbol_list}
        self.detected: dict = {sym: False for sym in bars.symbol_list}
        self.confirmed = {sym: False for sym in bars.symbol_list}
        self.bought: dict = {sym: False for sym in bars.symbol_list}
        self.pattern_state = {sym: "SCANNING" for sym in bars.symbol_list}

    def calculate_signals(self, event: Queue) -> None:
        """
        For "Buy and Hold" we generate a single signal per symbol
        and then no additional signals. This means we are
        constantly long the market from the date of strategy
        initialisation.

        Parameters
        event - A MarketEvent object.
        """
        #prvents system from buying multiple symbols in parallel if a position is already open
        #if any(self.bought.values()):
        #    return
        if event.type == 'MARKET':
            for s in self.symbol_list:
                # prevents system from buying same symbol multiple times if a position is already open
                if self.bought[s]:
                    continue
                bars = self.bars.get_latest_bars(s, 1)

                # get min max values and dates
                minima, maxima = self.get_min_max(self.latest_symbol_data[s])

                if len(minima) !=0 and len(maxima)!= 0:
                    if str(self.latest_symbol_data[s].index[-1]) == "2024-02-08 00:00:00":
                        pass
                        #self.plot_min_max(self.latest_symbol_data[s], minima, maxima)

                if self.pattern_state[s] == "SCANNING":
                    # Run scanner
                    pattern_dates = self.pattern_scanner(minima, maxima)
                    #ic(s, pattern_dates) # 1.1

                    #collect the pattern price points
                    price_data = self.get_PriceData(self.latest_symbol_data[s], pattern_dates)

                    #ic(s, price_data) # 1.2
                    if len(price_data) != 0:
                        # Initialize status columns for the new candidates
                        price_data['is_confirmed'] = False
                        price_data['is_bought'] = False
                        price_data['signal'] = np.nan
                        price_data['confirmation_date'] = pd.NaT

                        # Replace the state with the found patterns
                        self.pattern_data[s] = price_data
                        #ic(s, self.pattern_data[s]) # 1.3

                    if self.pattern_data[s]['is_detected'].any():
                        self.pattern_state[s] = "CONFIRMING"

                elif self.pattern_state[s] == "CONFIRMING":
                    # Store the information for confirmation with the rest of the pattern data
                    self.pattern_data[s] = self.get_ConfDate(self.latest_symbol_data[s], self.pattern_data[s])

                    if not self.pattern_data[s].empty and self.pattern_data[s]['is_confirmed'].any():
                        self.pattern_state[s] = "BUYING"

                elif self.pattern_state[s] == "BUYING":
                    if not self.bought[s]:
                        # Generate SHORT signal for double-top pattern
                        bars = self.bars.get_latest_bars(s, N=1)
                        signal = SignalEvent(symbol=s, timestamp=bars.index[0], signal_type='SHORT')
                        self.events.put(signal)

                        self.bought[s] = True
                        print(f"[doubleTop] Generated SHORT signal for {s}")

    def plot_min_max(self, data: pd.DataFrame, minima: float, maxima: float):
        # List of data points that fall under the minima category
        min_points = [minima.loc[k] if k in minima.index else np.nan for k in data.index]
        max_points = [maxima.loc[k] if k in maxima.index else np.nan for k in data.index]

        # Additional plots for marking the support and resistance levels
        apd = [mpf.make_addplot(min_points, type='scatter', color="green",marker='^', markersize=400),
               mpf.make_addplot(max_points, type='scatter', color="red", marker='v', markersize=400)]

        # Plot the OHLC data along with the lines passing through the nearest support and resistance levels
        mpf.plot(data, type='candle', style='classic', addplot=apd, title=str(data.index[-1]),figsize=(15, 7), block=False)
        plt.close()

    def get_min_max(self, df: pd.DataFrame, window: int = 10) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        This is a strict mathematical approach.

        How it works: It finds points that are strictly the maximum (or minimum) within a 
        window of order points on both sides.

        How it determines a peak: If you set order=10, a point is only considered a peak 
           if it is higher than the 10 bars before it AND the 10 bars after it.

        Verdict: Too Rigid. It tends to be "laggy" (because you need 10 bars after a high 
           to confirm it was a high) or it misses legitimate patterns because a single 
           noisy candle 5 bars later was slightly higher.

        """
        #use the argrelextrema to compute the local minima and maxima points
        #local_min = argrelextrema(df.iloc[:-argrel_window]['Low'].values,
        #                      np.less, order=argrel_window)[0]
        #local_max = argrelextrema(df.iloc[:-argrel_window]['High'].values,
        #                      np.greater, order=argrel_window)[0]

        """ 
        This is a simpler library often used in signal processing, but less common in financial 
        data now that scipy has improved.

        How it works: It generally finds local maxima and filters them based on a normalized threshold.
        Key Parameters:
         - thres (Threshold): A value between 0.0 and 1.0. It calculates the range of your data (Max - Min) 
           and effectively says "only keep peaks that are in the top X% of the price range".

        Verdict: Less Flexible. The threshold is absolute relative to the window. If you have a strong trend 
           where "tops" are lower than recent highs (like in a downtrend), this method might miss them because 
           they aren't in the "top 60%" of the window's range.
        """
        # Detect peaks (highs) and valleys (lows) using PeakUtils
        #peaks_idx = peakutils.indexes(df['High'], thres=0.60, min_dist=window)
        #valleys_idx = peakutils.indexes(-df['Low'], thres=0.60, min_dist=window)

        
        """
        This is the most modern and flexible method of the three. It works by comparing neighboring 
        values to find local maxima and then applying strict "properties" to filter them.

        How it works: 
            - It identifies any point that is higher than its immediate neighbors. 
            It then filters these points based on parameters like prominence (how much the 
            peak stands out from the surrounding "terrain") and distance.

        Key Parameters:
         - prominence: This is the vertical distance between the peak and its lowest contour line. 
            This is excellent for trading because it ignores "noisy" small peaks and only finds 
            "visually significant" tops.
         - distance: The minimum number of horizontal bars required between neighboring peaks.

        Verdict: Best for Trading. The "prominence" feature closely creates what a human eye would 
            see as a "top" or "bottom" on a chart.
        """        
        # Detect peaks (highs) and valleys (lows) using scipy.signal.find_peaks
        peaks_idx, _ = find_peaks(df['High'], height=None, prominence=0.5, distance=10)
        valleys_idx,_ = find_peaks(-df['Low'], height=None, prominence=0.5, distance=10)

        #store the minima and maxima values in a dataframe
        return  df.iloc[valleys_idx].Low,  df.iloc[peaks_idx].High

    def pattern_scanner(self, minima: pd.Series, maxima: pd.Series, frequency: str ='daily') -> list:
        # To store pattern instances
        patterns = []
        # Assuming minima and maxima are backtesting._util._Array objects
        # Convert them to pandas Series or DataFrame
        minima_series = pd.Series(minima)
        maxima_series= pd.Series(maxima)

        #concatinate both dataframes then sort them by index
        min_max = pd.concat([minima_series, maxima_series]).sort_index()

        # Loop to iterate along the price data
        for i in range(self.datapoints, len(min_max)+1):
            # Store 3 local minima and local maxima points at a time in the variable 'window'
            window = min_max.iloc[i-self.datapoints:i]

            # Determine window length based on the frequency of data
            window_size = (window.index[-1] - window.index[0]).days

            # Ensure that pattern is formed within 100 bars
            if window_size > 100:
                continue

            # Store the 3 unique points to check for conditions
            A, B, C = [window.iloc[i] for i in range(0, len(window))]

            # cond_1: To check b is in minima
            cond_1 = B in minima.values

            # cond_2: To check a,c are in maxima_prices
            cond_2 = all(x in maxima.values for x in [A, C])

            # cond_3: To check if the tops are above the neckline
            cond_3 = (B < A) and (B < C)

            # cond_4: To check if A and C are at a distance less than 1.5% away from their mean
            cond_4 = abs(A-C) <= np.mean([A, C]) * 0.1

            # Checking if all conditions are true
            if cond_1 and cond_2 and cond_3 and cond_4:
                # Append the pattern to list if all conditions are met
                patterns.append( ( [window.index[i] for i in range(0, len(window))] ) )

        return patterns

    def get_PriceData(self, data: pd.DataFrame, pattern_list: list ) -> pd.DataFrame :
        pattern_data = pd.DataFrame(pattern_list, columns = ['top1_date', 'neck1_date', 'top2_date'])

        # Populate the dataframe with relevant values
        pattern_data['top1_price'] = data.loc[pattern_data.top1_date, 'High'].values
        pattern_data['neck1_price'] = data.loc[pattern_data.neck1_date, 'Low'].values
        pattern_data['top2_price'] = data.loc[pattern_data.top2_date, 'High'].values
        pattern_data['is_detected'] = True

        return pattern_data

    def get_ConfDate(self, data: pd.DataFrame , pattern_data: pd.DataFrame):
        # If not empty
        if len(pattern_data) != 0:
            if 'confirmation_date' not in pattern_data.columns:
                pattern_data['confirmation_date'] = pd.NaT
                pattern_data['confirmation_date'] = pattern_data['confirmation_date'].astype('object')

            for x in range(0, len(pattern_data)):
                # store the data after second top in 'data_after_top2'
                data_after_top2 = data.loc[pattern_data.at[x, 'top2_date'] : ]['Close']

                try:
                    # return the short entry date if price went below the neckline
                     pattern_data.at[x,'confirmation_date'] = data_after_top2[data_after_top2 < pattern_data.at[x,'neck1_price']].index[0]

                     pattern_data[['confirmation_date']] = pattern_data[['confirmation_date']].apply(pd.to_datetime, format='%Y-%m-%d')

                     # Store the number of days taken to generate a short entry date in the column 'time_for_confirmation'
                     pattern_data.at[x,'time_for_confirmation'] = (pattern_data.at[x,'confirmation_date'] - pattern_data.at[x,'top2_date']).days

                except:
                    # return nan if price never went below the neckline
                    pattern_data.at[x,'confirmation_date'] = np.nan

            pattern_data['signal'] = -1
            
            # Set is_confirmed based on whether a confirmation date was found
            pattern_data['is_confirmed'] = pd.notna(pattern_data['confirmation_date'])
            
            num_confirmed = pattern_data['is_confirmed'].sum()
            if num_confirmed > 0:
                print(f"[doubleTop] Pattern confirmed! Found {num_confirmed} double top pattern(s)")

            # Only drop if we want to cleanup INVALID patterns? 
            # For now, let's keep them so we can confirm them later?
            # Actually, if we want to "wait", we shouldn't drop.
            # But we might want to drop only if they are somehow "too old"? 
            # For now, just removing dropna allows 'waiting'.
            
            pattern_data.reset_index(drop=True, inplace = True)

        return pattern_data
        
        return pattern_data

    def risk_Manager(self, pattern_data: pd.DataFrame):
        # If not empty
        if len(pattern_data) != 0:

            for x in range(0, len(pattern_data)):

                # Set stop-loss 1% above the right shoulder
                pattern_data.at[x,'stoploss'] = round(pattern_data.at[x,'top2_price']*1.01 , 2)

                # Calculate the distance between the head and the neckline
                pattern_data.at[x,'top_length'] = round(pattern_data.at[x,'top2_price'] - pattern_data.at[x,'neck1_price'] ,2)

                # Set target at a distance of head_length below the neckline
                pattern_data.at[x,'target'] = round(pattern_data.at[x,'neck1_price'] -  1 * pattern_data.at[x,'top_length'],2)
