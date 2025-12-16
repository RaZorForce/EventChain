from fastapi import FastAPI, BackgroundTasks
import threading
import queue
import os
import sys

# Add backend root to sys.path to allow imports
backend_root = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
if backend_root not in sys.path:
    sys.path.append(backend_root)

from src.config import SYMBOLS, CSV_DIR, INITIAL_CAPITAL, STRATEGY_NAME, STRATEGY_REGISTRY
from src.engine import TradingEngine
from src.data_handler import HistoricCSVDataHandler
from src.portfolio import BasicPortfolio, PercentSizer
from src.broker import SimulatedExecutionHandler

app = FastAPI()

# Global state
engine_thread = None
engine = None
bars = None

class TradingSystem:
    def __init__(self):
        self.events = queue.Queue()
        csv_dir = os.path.join(backend_root, CSV_DIR)
        self.bars = HistoricCSVDataHandler(self.events, csv_dir, SYMBOLS)
        
        StrategyClass = STRATEGY_REGISTRY[STRATEGY_NAME]
        self.strategy = StrategyClass(self.bars, self.events)
        
        sizer = PercentSizer(0.10)
        self.portfolio = BasicPortfolio(self.bars, self.events, INITIAL_CAPITAL, sizer=sizer)
        self.broker = SimulatedExecutionHandler(self.events)
        
        self.engine = TradingEngine(self.bars, self.strategy, self.portfolio, self.broker)
        self.is_running = False

    def start(self):
        self.is_running = True
        self.engine.run()
        self.is_running = False

system = None

@app.on_event("startup")
def startup_event():
    global system
    system = TradingSystem()

@app.get("/status")
def get_status():
    if not system:
        return {"status": "not_initialized"}
    
    # Return some portfolio metrics
    equity = system.portfolio.total_equity if system.portfolio else INITIAL_CAPITAL
    return {
        "status": "running" if system.is_running or (system.bars and system.bars.continue_backtest) else "stopped",
        "equity": equity,
        "holdings": system.portfolio.current_positions if system.portfolio else {}
    }

@app.post("/start")
def start_trading(background_tasks: BackgroundTasks):
    global engine_thread
    if system.is_running:
        return {"message": "Already running"}
    
    # Reset bars if needed or create new system (simple implementation: assume fresh start)
    # For backtest, we might need to reset everything. 
    # For now, just start.
    
    background_tasks.add_task(system.start)
    return {"message": "Started"}

@app.post("/stop")
def stop_trading():
    if system and system.bars:
        system.bars.continue_backtest = False
    return {"message": "Stopping..."}
