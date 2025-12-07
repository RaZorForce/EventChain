# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
from queue import Queue
from typing import Tuple

from src.data_handler import DataHandler
from .base import Patterns


class tripleTop(Patterns):

    def __init__(self, bars: DataHandler, events: Queue) -> None:
        super().__init__(bars, events)
        self.name = "Triple Top"
        self.datapoints = 5
        self.bias = "Short"

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

            # cond_1: A, C, E are in maxima (tops)
            cond_1 = all(x in maxima.values for x in [A, C, E])

            # cond_2: B, D are in minima (necklines)
            cond_2 = all(x in minima.values for x in [B, D])

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
