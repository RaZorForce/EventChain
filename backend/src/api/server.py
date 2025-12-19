from fastapi import FastAPI, BackgroundTasks, UploadFile, File, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
import threading
import queue
import os
import sys
import json
import csv
import io
from pathlib import Path
from datetime import datetime
from typing import Optional

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

# Add CORS middleware for frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173", "http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Data directories
DATA_DIR = Path(backend_root) / "data"
HISTORICAL_DIR = DATA_DIR / "Historical" / "Daily"
UNIVERSES_DIR = DATA_DIR / "Universes"

# Ensure directories exist
HISTORICAL_DIR.mkdir(parents=True, exist_ok=True)
UNIVERSES_DIR.mkdir(parents=True, exist_ok=True)

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


# ============================================
# Universe Upload Endpoints
# ============================================

def detect_csv_format(header: list[str]) -> str:
    """Detect if CSV is symbol list or OHLCV data."""
    header_lower = [h.lower().strip() for h in header]

    # Check for OHLCV format - common column names for price data
    ohlcv_columns = {'date', 'datetime', 'time', 'open', 'high', 'low', 'close', 'volume', 'adj close'}
    # Need at least Open, High, Low, Close (OHLC) - Volume and Date are optional
    required_ohlc = {'open', 'high', 'low', 'close'}
    if required_ohlc.issubset(set(header_lower)):
        return 'ohlcv'

    # Check for symbol list format
    symbol_columns = {'ticker', 'symbol'}
    if any(col in header_lower for col in symbol_columns):
        return 'symbol_list'

    return 'unknown'


def parse_filename_info(filename: str) -> dict:
    """
    Extract ticker symbol and resolution from filename.
    Expected formats:
    - AAPL_Daily.csv -> ticker=AAPL, resolution=daily
    - AAPL_1m.csv -> ticker=AAPL, resolution=1m
    - AAPL_1h.csv -> ticker=AAPL, resolution=1h
    - AAPL.csv -> ticker=AAPL, resolution=daily (default)
    - AAPL Daily Bars.csv -> ticker=AAPL, resolution=daily
    """
    # Remove .csv extension
    name = filename.rsplit('.', 1)[0]

    # Common resolution patterns
    resolution_patterns = {
        'daily': ['daily', 'day', '1d', 'd'],
        '1m': ['1m', '1min', '1minute'],
        '5m': ['5m', '5min', '5minute'],
        '15m': ['15m', '15min', '15minute'],
        '30m': ['30m', '30min', '30minute'],
        '1h': ['1h', '1hr', '1hour', 'hourly'],
        '4h': ['4h', '4hr', '4hour'],
    }

    # Try to split by common delimiters
    parts = name.replace('-', '_').replace(' ', '_').split('_')

    ticker = None
    resolution = 'daily'  # Default resolution

    if len(parts) >= 1:
        # First part is usually the ticker
        ticker = parts[0].upper()

        # Look for resolution in remaining parts
        for part in parts[1:]:
            part_lower = part.lower()
            for res, patterns in resolution_patterns.items():
                if part_lower in patterns or any(p in part_lower for p in patterns):
                    resolution = res
                    break

    # Clean up ticker - remove any non-alphanumeric characters
    if ticker:
        ticker = ''.join(c for c in ticker if c.isalnum())

    return {
        'ticker': ticker or 'UNKNOWN',
        'resolution': resolution
    }


def parse_symbol_list_csv(content: str) -> list[dict]:
    """Parse a CSV containing a list of symbols."""
    reader = csv.DictReader(io.StringIO(content))
    symbols = []

    for row in reader:
        # Find ticker column (case-insensitive)
        ticker = None
        name = None
        sector = None
        market_cap = None

        for key, value in row.items():
            key_lower = key.lower().strip()
            if key_lower in ('ticker', 'symbol'):
                ticker = value.strip().upper()
            elif key_lower in ('name', 'company'):
                name = value.strip()
            elif key_lower in ('sector', 'industry'):
                sector = value.strip()
            elif key_lower in ('marketcap', 'market_cap', 'market cap'):
                market_cap = value.strip()

        if ticker:
            symbols.append({
                'ticker': ticker,
                'name': name or ticker,
                'sector': sector,
                'marketCap': market_cap
            })

    return symbols


def validate_ohlcv_csv(content: str) -> tuple[bool, str, int]:
    """Validate OHLCV CSV format. Returns (is_valid, message, row_count)."""
    try:
        reader = csv.DictReader(io.StringIO(content))
        rows = list(reader)

        if not rows:
            return False, "CSV file is empty", 0

        # Check required columns - only OHLC are strictly required
        header = [h.lower() for h in reader.fieldnames or []]
        required_ohlc = {'open', 'high', 'low', 'close'}
        missing = required_ohlc - set(header)

        if missing:
            return False, f"Missing required OHLC columns: {', '.join(missing)}", 0

        return True, f"Valid OHLCV data with {len(rows)} rows", len(rows)
    except Exception as e:
        return False, str(e), 0


@app.post("/api/data/upload-universe")
async def upload_universe(
    file: UploadFile = File(...),
    name: str = Form(...),
    description: str = Form("")
):
    """
    Upload a CSV file containing universe data.
    Supports two formats:
    1. Symbol list: ticker,name,sector,marketcap
    2. OHLCV data: Date,Open,High,Low,Close,Volume
    """
    if not file.filename or not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Only CSV files are accepted")

    try:
        content = (await file.read()).decode('utf-8')
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to read file: {str(e)}")

    # Parse header
    lines = content.strip().split('\n')
    if len(lines) < 2:
        raise HTTPException(status_code=400, detail="CSV must have at least a header and one data row")

    header = lines[0].split(',')
    csv_format = detect_csv_format(header)

    if csv_format == 'unknown':
        raise HTTPException(
            status_code=400,
            detail="Unknown CSV format. Expected either symbol list (ticker,name,...) or OHLCV (Date,Open,High,Low,Close,Volume)"
        )

    # Create universe metadata
    universe_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_name = "".join(c if c.isalnum() or c in '-_' else '_' for c in name)
    universe_folder = UNIVERSES_DIR / f"{safe_name}_{universe_id}"
    universe_folder.mkdir(parents=True, exist_ok=True)

    result = {
        "status": "success",
        "universe_id": universe_id,
        "name": name,
        "format": csv_format,
        "path": str(universe_folder),
    }

    if csv_format == 'symbol_list':
        # Parse and save symbol list
        symbols = parse_symbol_list_csv(content)

        if not symbols:
            raise HTTPException(status_code=400, detail="No valid symbols found in CSV")

        # Save manifest
        manifest = {
            "id": universe_id,
            "name": name,
            "description": description,
            "created_at": datetime.now().isoformat(),
            "format": "symbol_list",
            "symbol_count": len(symbols),
            "symbols": symbols,
        }

        manifest_path = universe_folder / "manifest.json"
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)

        # Also save original CSV
        csv_path = universe_folder / "symbols.csv"
        with open(csv_path, 'w') as f:
            f.write(content)

        result["symbols_count"] = len(symbols)
        result["symbols"] = symbols

    elif csv_format == 'ohlcv':
        # Validate OHLCV data
        is_valid, message, row_count = validate_ohlcv_csv(content)

        if not is_valid:
            raise HTTPException(status_code=400, detail=message)

        # Extract ticker symbol and resolution from filename
        file_info = parse_filename_info(file.filename or "")
        ticker = file_info['ticker']
        resolution = file_info['resolution']

        # Determine the appropriate subdirectory based on resolution
        if resolution == 'daily':
            data_dir = DATA_DIR / "Historical" / "Daily"
            ohlcv_filename = f"{ticker}_Daily_Bars.csv"
        else:
            # For intraday data, use Intraday folder with resolution suffix
            data_dir = DATA_DIR / "Historical" / "Intraday"
            ohlcv_filename = f"{ticker}_{resolution}_Bars.csv"

        data_dir.mkdir(parents=True, exist_ok=True)
        ohlcv_path = data_dir / ohlcv_filename

        with open(ohlcv_path, 'w') as f:
            f.write(content)

        # Also save manifest for tracking
        manifest = {
            "id": universe_id,
            "name": name,
            "description": description,
            "created_at": datetime.now().isoformat(),
            "format": "ohlcv",
            "ticker": ticker,
            "resolution": resolution,
            "row_count": row_count,
            "file_path": str(ohlcv_path),
            "original_filename": file.filename,
        }

        manifest_path = universe_folder / "manifest.json"
        with open(manifest_path, 'w') as f:
            json.dump(manifest, f, indent=2)

        result["ticker"] = ticker
        result["resolution"] = resolution
        result["row_count"] = row_count
        result["ohlcv_path"] = str(ohlcv_path)
        result["message"] = f"Saved {ticker} {resolution} data with {row_count} bars"

    return result


@app.get("/api/data/universes")
async def list_universes():
    """List all saved universes."""
    universes = []

    if UNIVERSES_DIR.exists():
        for folder in UNIVERSES_DIR.iterdir():
            if folder.is_dir():
                manifest_path = folder / "manifest.json"
                if manifest_path.exists():
                    try:
                        with open(manifest_path, 'r') as f:
                            manifest = json.load(f)
                            universes.append(manifest)
                    except Exception:
                        pass

    # Sort by creation date, newest first
    universes.sort(key=lambda x: x.get('created_at', ''), reverse=True)

    return {"universes": universes}


@app.delete("/api/data/universe/{universe_id}")
async def delete_universe(universe_id: str):
    """Delete a saved universe."""
    import shutil

    # Find universe folder by ID
    target_folder = None

    if UNIVERSES_DIR.exists():
        for folder in UNIVERSES_DIR.iterdir():
            if folder.is_dir() and universe_id in folder.name:
                manifest_path = folder / "manifest.json"
                if manifest_path.exists():
                    with open(manifest_path, 'r') as f:
                        manifest = json.load(f)
                        if manifest.get('id') == universe_id:
                            target_folder = folder

                            # If OHLCV, also delete from Historical/Daily
                            if manifest.get('format') == 'ohlcv' and manifest.get('file_path'):
                                ohlcv_path = Path(manifest['file_path'])
                                if ohlcv_path.exists():
                                    ohlcv_path.unlink()
                            break

    if not target_folder:
        raise HTTPException(status_code=404, detail=f"Universe not found: {universe_id}")

    # Delete the folder
    shutil.rmtree(target_folder)

    return {"status": "success", "message": f"Universe {universe_id} deleted"}
