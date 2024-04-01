# -*- coding: utf-8 -*-

# datahandler.py

import os, os.path
import pandas as pd
from queue import Queue
from abc import ABCMeta, abstractmethod
from typing import Generator, Tuple
from event import MarketEvent
from icecream import ic

class DataHandler(object):
    """
    DataHandler is an abstract base class providing an interface for all
    subsequent (inherited) data handlers (both live and historic).

    The goal of a (derived) DataHandler object is to output a generated set of
    bars (OLHCV) for each symbol requested.

    This will replicate how a live strategy would function as current
    market data would be sent "down the pipe". Thus a historic and live
    system will be treated identically by the rest of the backtesting suite.
    """

    __metaclass__ = ABCMeta

    @abstractmethod
    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars from the latest_symbol list, or fewer if less
        bars are available.
        """
        raise NotImplementedError("Should implement get_latest_bars()")

    @abstractmethod
    def update_bars(self):
        """
        Pushes the latest bar to the latest symbol structure for all symbols
        in the symbol list.
        """
        raise NotImplementedError("Should implement update_bars()")

class HistoricCSVDataHandler(DataHandler):
    """
    HistoricCSVDataHandler is designed to read CSV files for each requested
    symbol from disk and provide an interface to obtain the "latest" bar in
    a manner identical to a live trading interface.
    """

    def __init__(self, events: Queue, csv_dir: str, symbol_list: list) -> None:
        """
        Initialises the historic data handler by requesting the location of
        the CSV files and a list of symbols.

        It will be assumed that all files are of the form 'symbol.csv',
        where symbol is a string in the list.

        Parameters:
        events - The Event Queue.
        csv_dir - Absolute directory path to the CSV files.
        symbol_list - A list of symbol strings.
        """
        self.events: Queue = events
        self.csv_dir:str = csv_dir
        self.symbol_list:list = symbol_list

        self.symbol_data:dict = {}
        self.latest_symbol_data:dict = {}
        self.continue_backtest: bool = True

        self._open_convert_csv_files()

    def _open_convert_csv_files(self) -> None:
        """
        Opens the CSV files from the data directory, converting
        them into pandas DataFrames within a symbol dictionary.

        For this handler it will be assumed that the data is
        taken from Yahoo. Thus its format will be respected.
        """
        comb_index = None

        for s in self.symbol_list:
            # Load the CSV file with no header information, indexed on date
            self.symbol_data[s] = pd.read_csv(
                os.path.join(self.csv_dir, s+'_Daily_Bars.csv'),
                header=0, index_col=0, parse_dates=True,
                names=['datetime', 'Open', 'High','Low', 'Close', 'Volume']
            )
            self.symbol_data[s].sort_index(inplace=True)

            # Combine the index to pad forward values
            if comb_index is None:
                comb_index = self.symbol_data[s].index
            else:
                comb_index.union(self.symbol_data[s].index)

            # Set the latest symbol_data to None
            self.latest_symbol_data[s] = pd.DataFrame()

            self.symbol_data[s] = self.symbol_data[s].iterrows()

# =============================================================================
#         for s in self.symbol_list:
#             symbol_data[s]["returns"] = symbol_data[s]["close"].pct_change().dropna()
#             # Reindex the dataframes
#             symbol_data[s] = symbol_data[s].reindex(index=comb_index, method='pad').iterrows()
# =============================================================================

    def _get_new_bar(self, symbol: str) -> Generator:
        """
        Returns the latest bar from the data feed as a tuple of
        (symbol, datetime, open, low, high, close, volume).
        """
        for index, row in self.symbol_data[symbol]:
            yield{"Date": index, "Open": row["Open"], "High": row["High"],
                   "Low": row["Low"], "Close": row["Close"], "Volume": row["Volume"]}


    def get_latest_bars(self, symbol: str, N: int =1) -> list:
        """
        Returns the last N bars from the latest_symbol list, or N-k if less
        available.
        """
        try:
            return self.latest_symbol_data[symbol].iloc[-N:]
        except KeyError:
            print (f"the symbol {symbol} is not available in the historical data set.")
            return None


    def update_bars(self) -> None:
        """
        Pushes the latest bar to the latest_symbol_data structure
        for all symbols in the symbol list.
        """
        for s in self.symbol_list:
            try:
                bar: dict = next(self._get_new_bar(s))

                if bar is not None:
                    bar_df = pd.DataFrame(bar,index=[0]).set_index('Date')
                    self.latest_symbol_data[s] = pd.concat([self.latest_symbol_data[s], bar_df])

            except StopIteration:
                self.continue_backtest = False

        #ic(self.latest_symbol_data)
        self.events.put(MarketEvent())