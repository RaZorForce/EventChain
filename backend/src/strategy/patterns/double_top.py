# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from queue import Queue
from typing import Tuple

from src.data_handler import DataHandler
from .base import Patterns


class doubleTop(Patterns):

    def __init__(self, bars: DataHandler, events: Queue) -> None:
        super().__init__(bars, events)
        self.name = "Double Top"
        self.datapoints = 3
        self.bias = "Short"

    def pattern_scanner(self, minima: pd.Series, maxima: pd.Series, frequency: str = 'daily') -> list:
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
            # Taken window for pattern recognition
            window = min_max.iloc[i-self.datapoints:i]
            
            # window_size is the duration of window
            window_size = (window.index[-1] - window.index[0]).days

            # Ensure the window duration is less than 100 days
            if window_size > 100:
                continue

            # A, B, C are the price points in the window
            A, B, C = [window.iloc[i] for i in range(0, len(window))]

            # cond_1: To check if A and C are in maxima
            cond_1 = all(x in maxima.values for x in [A, C])

            # cond_2: To check if B are in minima
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
                    # check if the data_after_top2 drops below the neckline price 'neck1_price'
                    # if so, get the first date where it drops below the neckline price 
                    pattern_data.at[x,'confirmation_date'] = data_after_top2[data_after_top2 < pattern_data.at[x,'neck1_price']].index[0]

                    # Store the time taken for confirmation in 'time_for_confirmation'
                    pattern_data[['confirmation_date']] = pattern_data[['confirmation_date']].apply(pd.to_datetime, format='%Y-%m-%d')
                    pattern_data.at[x,'time_for_confirmation'] = (pattern_data.at[x,'confirmation_date'] - pattern_data.at[x,'top2_date']).days

                except IndexError:
                    pattern_data.at[x,'confirmation_date'] = np.nan
                except:
                    pattern_data.at[x, 'confirmation_date'] = np.nan

            pattern_data['signal'] = -1
            
            # Set is_confirmed based on whether a confirmation date was found
            pattern_data['is_confirmed'] = pd.notna(pattern_data['confirmation_date'])
            
            num_confirmed = pattern_data['is_confirmed'].sum()
            if num_confirmed > 0:
                print(f"[doubleTop] Pattern confirmed! Found {num_confirmed} double top pattern(s)")
            
            pattern_data.reset_index(drop=True, inplace = True)

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
