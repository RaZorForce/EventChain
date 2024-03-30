# -*- coding: utf-8 -*-

# strategy.py

import datetime
import numpy as np
import pandas as pd
import queue
from queue import Queue
from abc import ABCMeta, abstractmethod
from datahandler import DataHandler
from event import SignalEvent
from typing import Tuple
from scipy.signal import argrelextrema

class Strategy(object):
    """
    Strategy is an abstract base class providing an interface for
    all subsequent (inherited) strategy handling objects.

    The goal of a (derived) Strategy object is to generate Signal
    objects for particular symbols based on the inputs of Bars
    (OLHCVI) generated by a DataHandler object.

    This is designed to work both with historic and live data as
    the Strategy object is agnostic to the data source,
    since it obtains the bar tuples from a queue object.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def calculate_signals(self):
        """
        Provides the mechanisms to calculate the list of signals.
        """
        raise NotImplementedError("Should implement calculate_signals()")


class BuyAndHoldStrategy(Strategy):
    """
    This is an extremely simple strategy that goes LONG all of the
    symbols as soon as a bar is received. It will never exit a position.

    It is primarily used as a testing mechanism for the Strategy class
    as well as a benchmark upon which to compare other strategies.
    """

    def __init__(self, bars: DataHandler, events: Queue) -> None:
        """
        Initialises the buy and hold strategy.

        Parameters:
        bars - The DataHandler object that provides bar information
        events - The Event Queue object.
        """
        self.bars: DataHandler = bars
        self.symbol_list: list = bars.symbol_list
        self.events: Queue = events

        # Once buy & hold signal is given, these are set to True
        self.bought: dict = self._calculate_initial_bought()

    def _calculate_initial_bought(self) -> dict:
        """
        Adds keys to the bought dictionary for all symbols
        and sets them to False.
        """
        bought: dict = {}
        for s in self.symbol_list:
            bought[s] = False
        return bought

    def calculate_signals(self, event: Queue) -> None:
        """
        For "Buy and Hold" we generate a single signal per symbol
        and then no additional signals. This means we are
        constantly long the market from the date of strategy
        initialisation.

        Parameters
        event - A MarketEvent object.
        """
        if event.type == 'MARKET':
            for s in self.symbol_list:
                bars: list = self.bars.get_latest_bars(s, N=2)
                if bars is not None and bars != []:
                    if self.bought[s] == False:
                        # (Symbol, Datetime, Type = LONG, SHORT or EXIT)
                        signal: SignalEvent = SignalEvent(bars[0][0], bars[0][1], 'LONG')
                        self.events.put(signal)
                        self.bought[s] = True

class doubleTop(Strategy):

    def __init__(self, bars: DataHandler, events: Queue) -> None:
        self.name = "Double Top"
        self.datapoints = 3
        self.bias = "Short"
        self.bars: DataHandler = bars
        self.symbol_list: list = bars.symbol_list
        self.latest_symbol_data: dict = bars.latest_symbol_data
        self.events: Queue = events

        self.highs = {key: [] for key in bars.latest_symbol_data.keys()}
        self.lows = {key: [] for key in bars.latest_symbol_data.keys()}

        # Once buy & hold signal is given, these are set to True
        self.bought: dict = self._calculate_initial_bought()
    def _calculate_initial_bought(self) -> dict:
        """
        Adds keys to the bought dictionary for all symbols
        and sets them to False.
        """
        bought: dict = {}
        for s in self.symbol_list:
            bought[s] = False
        return bought

    def calculate_signals(self, event: Queue) -> None:
        """
        For "Buy and Hold" we generate a single signal per symbol
        and then no additional signals. This means we are
        constantly long the market from the date of strategy
        initialisation.

        Parameters
        event - A MarketEvent object.
        """
        if event.type == 'MARKET':
            for s in self.symbol_list:
                bars: list = self.bars.get_latest_bars(s, 1)

                self.get_min_max(s, bars)



                if bars is not None and bars != []:
                    if self.bought[s] == False:
                        # (Symbol, Datetime, Type = LONG, SHORT or EXIT)
                        signal: SignalEvent = SignalEvent(bars[0][0], bars[0][1], 'LONG')
                        self.events.put(signal)
                        self.bought[s] = True

    def get_min_max(self, s, bars: dict, argrel_window: int = 5) -> Tuple[pd.DataFrame, pd.DataFrame]:

        #use the argrelextrema to compute the local minima and maxima points
        self.lows[s].append(bars[0][4])# = np.array([tup[4] for tup in bars])
        local_min = argrelextrema(np.array(self.lows[s]), np.less, order=argrel_window)[0]

        self.highs[s].append(bars[0][3]) # = np.array([tup[3] for tup in bars])
        local_max = argrelextrema(np.array(self.highs[s]), np.greater, order=argrel_window)[0]

        if local_min.size > 0 or local_max.size > 0:
            x=0

        #store the minima and maxima values in a dataframe
        #return  df.iloc[local_min].Low,  df.iloc[local_max].High

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
        for i in range(self.datapoints, len(min_max)):

            # Store 3 local minima and local maxima points at a time in the variable 'window'
            window = min_max.iloc[i-self.datapoints:i]

            # Determine window length based on the frequency of data
            window_size = (window.index[-1] - window.index[0])

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
                patterns.append(
                    ([window.index[i] for i in range(0, len(window))]))

        print(f"Double Top pattern detected {len(patterns)} times ")

        return patterns

    def get_PriceData(self, data: pd.DataFrame, pattern_list: list ) -> pd.DataFrame :
        pattern_data = pd.DataFrame(pattern_list, columns = ['top1_date', 'neck1_date', 'top2_date'])

        # Populate the dataframe with relevant values
        pattern_data['top1_price'] = data.loc[pattern_data.top1_date, 'High'].values
        pattern_data['neck1_price'] = data.loc[pattern_data.neck1_date, 'Low'].values
        pattern_data['top2_price'] = data.loc[pattern_data.top2_date, 'High'].values

        return pattern_data

    def get_ConfDate(self, data: pd.DataFrame , pattern_data: pd.DataFrame):
        # If not empty
        if len(pattern_data) != 0:

            for x in range(0, len(pattern_data)):
                # store the data after second top in 'data_after_top2'
                data_after_top2 = data.loc[pattern_data.at[x, 'top2_date'] : ]['Close']

                try:
                    # return the short entry date if price went below the neckline
                     pattern_data.at[x,'confirmation_date'] = data_after_top2[data_after_top2 < pattern_data.at[x,'neck1_price']].index[0]

                     #pattern_data[['confirmation_date']] = pattern_data[['confirmation_date']].apply(pd.to_datetime, format='%Y-%m-%d')

                     # Store the number of days taken to generate a short entry date in the column 'time_for_confirmation'
                     pattern_data.at[x,'time_for_confirmation'] = (pattern_data.at[x,'confirmation_date'] - pattern_data.at[x,'top2_date']).days

                except:
                    # return nan if price never went below the neckline
                    pattern_data.at[x,'confirmation_date'] = np.nan

            pattern_data['signal'] = -1

            # Drop NaN values from 'hs_patterns_data'
            pattern_data.dropna(inplace=True)

            pattern_data.reset_index(drop=True, inplace = True)

            # Selecting the patterns that represent head and shoulders patterns that can be traded
            #pattern_data = pattern_data[(pattern_data['time_for_confirmation'] > 5) & ( pattern_data['time_for_confirmation'] < 30)]

        print(f"Double Top pattern confirmed {len(pattern_data)} times")

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