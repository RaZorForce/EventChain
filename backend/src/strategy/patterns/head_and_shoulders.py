# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from queue import Queue
from typing import Tuple

from src.data_handler import DataHandler
from .base import Patterns


class headAndShoulders(Patterns):
    r"""
    Head and Shoulders pattern (Bearish reversal).

    Pattern Shape:
              C           <- Head (highest maxima)
             / \
        A  /   \  E       <- Left shoulder, Right shoulder (maxima)
         \/     \/
          B     D         <- Neckline points (minima)

    Confirmation: Price closes BELOW neckline (neck2)
    Signal: SHORT
    """

    def __init__(self, bars: DataHandler, events: Queue) -> None:
        super().__init__(bars, events)
        self.name = "Head and Shoulders"
        self.datapoints = 5
        self.bias = "Short"

    def pattern_scanner(self, minima: pd.Series, maxima: pd.Series, frequency: str = 'daily') -> list:
        """
        Scan for head and shoulders pattern: A-B-C-D-E where:
        - A, C, E are maxima (left shoulder, head, right shoulder)
        - B, D are minima (neckline points)
        - C > A and C > E (head is highest)
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

            # cond_1: A, C, E are in maxima (shoulders and head)
            cond_1 = all(x in maxima.values for x in [A, C, E])

            # cond_2: B, D are in minima (neckline)
            cond_2 = all(x in minima.values for x in [B, D])

            # cond_3: Head (C) is above shoulders, shoulders are above neckline
            cond_3 = (C > A) and (C > E) and (A > B) and (A > D) and (E > B) and (E > D)

            # cond_4: Shoulders within 20% of each other, necklines within 20% (relaxed from 10%)
            cond_4 = (abs(A - E) <= np.mean([A, E]) * 0.2) and (abs(B - D) <= np.mean([B, D]) * 0.2)

            if cond_1 and cond_2 and cond_3 and cond_4:
                patterns.append([window.index[j] for j in range(0, len(window))])

        return patterns

    def get_PriceData(self, data: pd.DataFrame, pattern_list: list) -> pd.DataFrame:
        pattern_data = pd.DataFrame(pattern_list, columns=['sh1_date', 'neck1_date', 'head_date', 'neck2_date', 'sh2_date'])

        pattern_data['sh1_price'] = data.loc[pattern_data.sh1_date, 'High'].values
        pattern_data['neck1_price'] = data.loc[pattern_data.neck1_date, 'Low'].values
        pattern_data['head_price'] = data.loc[pattern_data.head_date, 'High'].values
        pattern_data['neck2_price'] = data.loc[pattern_data.neck2_date, 'Low'].values
        pattern_data['sh2_price'] = data.loc[pattern_data.sh2_date, 'High'].values
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
                data_after_sh2 = data.loc[pattern_data.at[x, 'sh2_date']:]['Close']

                try:
                    # Confirm when price closes BELOW neck2
                    pattern_data.at[x, 'confirmation_date'] = data_after_sh2[
                        data_after_sh2 < pattern_data.at[x, 'neck2_price']
                    ].index[0]

                    pattern_data[['confirmation_date']] = pattern_data[['confirmation_date']].apply(pd.to_datetime, format='%Y-%m-%d')

                    pattern_data.at[x, 'time_for_confirmation'] = (
                        pattern_data.at[x, 'confirmation_date'] - pattern_data.at[x, 'sh2_date']
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
                print(f"[headAndShoulders] Pattern confirmed! Found {num_confirmed} head and shoulders pattern(s)")

            pattern_data.reset_index(drop=True, inplace=True)

        return pattern_data

    def risk_Manager(self, pattern_data: pd.DataFrame):
        if len(pattern_data) != 0:
            for x in range(0, len(pattern_data)):
                # Stop-loss 1% above the right shoulder
                pattern_data.at[x, 'stoploss'] = round(pattern_data.at[x, 'sh2_price'] * 1.01, 2)

                # Calculate head length (head to neckline distance)
                pattern_data.at[x, 'head_length'] = round(
                    pattern_data.at[x, 'head_price'] - pattern_data.at[x, 'neck2_price'], 2
                )

                # Target: head length projected below neckline
                pattern_data.at[x, 'target'] = round(
                    pattern_data.at[x, 'neck2_price'] - pattern_data.at[x, 'head_length'], 2
                )
