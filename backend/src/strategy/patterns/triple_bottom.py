# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from queue import Queue
from typing import Tuple

from src.data_handler import DataHandler
from .base import Patterns


class tripleBottom(Patterns):

    def __init__(self, bars: DataHandler, events: Queue) -> None:
        super().__init__(bars, events)
        self.name = "Triple Bottom"
        self.datapoints = 5
        self.bias = "Long"

    def pattern_scanner(self, minima: pd.Series, maxima: pd.Series, frequency: str = 'daily') -> list:
        """
        Scan for triple bottom pattern: A-B-C-D-E where A,C,E are bottoms and B,D are necklines.
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

            # cond_1: B, D are in maxima (necklines/resistance)
            cond_1 = all(x in maxima.values for x in [B, D])

            # cond_2: A, C, E are in minima (bottoms)
            cond_2 = all(x in minima.values for x in [A, C, E])

            # cond_3: Necklines are above the bottoms
            cond_3 = (B > A) and (B > C) and (D > E) and (D > C)

            # cond_4: Bottoms within 15% of each other, necklines within 15% (relaxed from 1.5%)
            cond_4 = (abs(A - C) <= np.mean([A, C]) * 0.15) and \
                     (abs(C - E) <= np.mean([C, E]) * 0.15) and \
                     (abs(B - D) <= np.mean([B, D]) * 0.15)

            if cond_1 and cond_2 and cond_3 and cond_4:
                patterns.append([window.index[j] for j in range(0, len(window))])

        return patterns

    def get_PriceData(self, data: pd.DataFrame, pattern_list: list) -> pd.DataFrame:
        pattern_data = pd.DataFrame(pattern_list, columns=['bottom1_date', 'neck1_date', 'bottom2_date', 'neck2_date', 'bottom3_date'])

        pattern_data['bottom1_price'] = data.loc[pattern_data.bottom1_date, 'Low'].values
        pattern_data['neck1_price'] = data.loc[pattern_data.neck1_date, 'High'].values
        pattern_data['bottom2_price'] = data.loc[pattern_data.bottom2_date, 'Low'].values
        pattern_data['neck2_price'] = data.loc[pattern_data.neck2_date, 'High'].values
        pattern_data['bottom3_price'] = data.loc[pattern_data.bottom3_date, 'Low'].values
        pattern_data['is_detected'] = True

        return pattern_data

    def get_ConfDate(self, data: pd.DataFrame, pattern_data: pd.DataFrame):
        """
        Check if price closed above neckline (confirmation of breakout).
        """
        if len(pattern_data) != 0:
            if 'confirmation_date' not in pattern_data.columns:
                pattern_data['confirmation_date'] = pd.NaT
                pattern_data['confirmation_date'] = pattern_data['confirmation_date'].astype('object')

            for x in range(0, len(pattern_data)):
                data_after_bottom3 = data.loc[pattern_data.at[x, 'bottom3_date']:]['Close']

                try:
                    # Confirm when price closes ABOVE neck2
                    pattern_data.at[x, 'confirmation_date'] = data_after_bottom3[
                        data_after_bottom3 > pattern_data.at[x, 'neck2_price']
                    ].index[0]

                    pattern_data[['confirmation_date']] = pattern_data[['confirmation_date']].apply(pd.to_datetime, format='%Y-%m-%d')

                    pattern_data.at[x, 'time_for_confirmation'] = (
                        pattern_data.at[x, 'confirmation_date'] - pattern_data.at[x, 'bottom3_date']
                    ).days

                except IndexError:
                    pattern_data.at[x, 'confirmation_date'] = np.nan
                except:
                    pattern_data.at[x, 'confirmation_date'] = np.nan

            pattern_data['signal'] = 1

            # Set is_confirmed based on whether a confirmation date was found
            pattern_data['is_confirmed'] = pd.notna(pattern_data['confirmation_date'])
            
            num_confirmed = pattern_data['is_confirmed'].sum()
            if num_confirmed > 0:
                print(f"[tripleBottom] Pattern confirmed! Found {num_confirmed} triple bottom pattern(s)")

            pattern_data.reset_index(drop=True, inplace=True)

        return pattern_data

    def risk_Manager(self, pattern_data: pd.DataFrame):
        if len(pattern_data) != 0:
            for x in range(0, len(pattern_data)):
                # Stop-loss 1% below the third bottom
                pattern_data.at[x, 'stoploss'] = round(pattern_data.at[x, 'bottom3_price'] * 0.99, 2)

                # Calculate pattern height
                pattern_data.at[x, 'bottom_length'] = round(
                    pattern_data.at[x, 'neck2_price'] - pattern_data.at[x, 'bottom3_price'], 2
                )

                # Target: pattern height projected above neckline
                pattern_data.at[x, 'target'] = round(
                    pattern_data.at[x, 'neck2_price'] + pattern_data.at[x, 'bottom_length'], 2
                )
