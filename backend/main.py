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
csv_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "Historical", "Daily")

#filenames = glob.glob("*_Daily_Bars.csv")
filenames = ["HUMA_Daily_Bars.csv", "AMRN_Daily_Bars.csv","ADPT_Daily_Bars.csv", "ALEC_Daily_Bars.csv"]
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
        except queue.Empty:
            break
        else:
            if event is not None:
                if event.type == 'MARKET':
                    strategy.calculate_signals(event)
                    portfolio.update_timeindex(event)
                    eventsQ.task_done()

                elif event.type == 'SIGNAL':
                    print(f"[SIGNAL] {event.signal_type} signal for {event.symbol} | Queue: {eventsQ.qsize()} remaining")
                    portfolio.update_signal(event)
                    eventsQ.task_done()

                elif event.type == 'ORDER':
                    print(f"[ORDER]  {event.direction} {event.quantity} shares of {event.symbol} | Queue: {eventsQ.qsize()} remaining")
                    broker.execute_order(event)
                    eventsQ.task_done()

                elif event.type == 'FILL':
                    print(f"[FILL]   {event.direction} {event.quantity} shares of {event.symbol} @ ${event.fill_cost:.2f} | Queue: {eventsQ.qsize()} remaining")
                    portfolio.update_fill(event)
                    eventsQ.task_done()

# Generate performance metrics and display results
print("\n" + "="*60)
print(" "*20 + "BACKTEST RESULTS")
print("="*60)

portfolio.create_equity_curve_dataframe()
stats = portfolio.output_summary_stats()

for stat in stats:
    print(stat)