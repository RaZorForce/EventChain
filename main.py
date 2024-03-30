# -*- coding: utf-8 -*-
import time
import queue

import os
import glob
from datahandler import HistoricCSVDataHandler
from strategy import Strategy, BuyAndHoldStrategy, doubleTop
from portfolio import Portfolio, NaivePortfolio
from execution import ExecutionHandler, SimulatedExecutionHandler

# Collect all filenames in current directory
csv_dir = "C:\\X\\Workspaces\\ALGO\\Data\\Historical\\Daily"
os.chdir(csv_dir)
filenames = glob.glob("*_Daily_Bars.csv")
symbol_list = [filename.split("_")[0] for filename in filenames]

# Declare the components with respective parameters
eventsQ = queue.Queue()

# Initialize objects
bars = HistoricCSVDataHandler(eventsQ, csv_dir, symbol_list)
strategy = doubleTop(bars,eventsQ)
portfolio = NaivePortfolio(bars, eventsQ, "20230215")
broker = SimulatedExecutionHandler(eventsQ)


while True:
    # Update the bars (specific backtest code, as opposed to live trading)
    if bars.continue_backtest == True:
        bars.update_bars()
    else:
        break

    # Handle the events
    while True:
        try:
            event = eventsQ.get(False)
            if event.type != "MARKET":
                print(f"queue size {eventsQ.qsize()} with event {event.type}")
                print(f"queue has {eventsQ.unfinished_tasks} unfinished tasks")
        except queue.Empty:
            break
        else:
            if event is not None:
                if event.type == 'MARKET':
                    strategy.calculate_signals(event)
                    portfolio.update_timeindex(event)
                    eventsQ.task_done()

                elif event.type == 'SIGNAL':
                    portfolio.update_signal(event)
                    eventsQ.task_done()

                elif event.type == 'ORDER':
                    broker.execute_order(event)
                    eventsQ.task_done()

                elif event.type == 'FILL':
                    portfolio.update_fill(event)
                    eventsQ.task_done()

    x= 0
# 10-Minute heartbeat
    #time.sleep(2)