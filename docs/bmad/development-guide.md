# Development Guide

## Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| Python | 3.13+ | Required for type hints and features |
| uv | Latest | Python package manager |
| Git | 2.x | Version control |

## Environment Setup

### 1. Clone Repository

```bash
git clone <repository-url>
cd EventChainTrader
```

### 2. Install Dependencies

```bash
cd backend
uv sync
```

This will create a virtual environment and install all dependencies from `pyproject.toml`.

### 3. Verify Installation

```bash
uv run python -c "from src import TradingEngine; print('OK')"
```

## Project Structure

```
EventChainTrader/
├── backend/
│   ├── config/          # Configuration files
│   ├── data/            # Market data (not in repo)
│   └── src/             # Source code
└── docs/                # Documentation
```

## Running the System

### Basic Backtest

```bash
cd backend
uv run python -m src.main
```

### With Custom Configuration

```bash
CONFIG_NAME=live uv run python -m src.main
```

### Expected Output

```
[main] Using strategy: Double Top
[SIGNAL] SHORT signal for NVTS | Queue: 0 remaining
[ORDER]  SELL 100 shares of NVTS | Queue: 0 remaining
[EXECUTION] FILLED: SELL 100 shares of NVTS | Queue: 0 remaining

============================================================
                    BACKTEST RESULTS
============================================================
('Total Return', '12.34%')
('Sharpe Ratio', '1.23')
('Max Drawdown', '5.67%')
('Drawdown Duration', '45')
```

## Configuration

### Configuration Files

| File | Purpose |
|------|---------|
| `config/backtest.yaml` | Backtest mode settings |
| `config/forwardtest.yaml` | Forward test settings |
| `config/paper.yaml` | Paper trading settings |
| `config/live.yaml` | Live trading settings |
| `config/event_bus.yaml` | Event routing configuration |

### Backtest Configuration Options

```yaml
# Strategy to use (from STRATEGY_REGISTRY)
strategy: double_top

# Symbols to trade
symbols:
  - NVTS
  - AAPL

# Data source
data:
  csv_dir: data/Historical/Daily
  start_date: "20230215"

# Portfolio settings
portfolio:
  initial_capital: 100000.0
```

### Available Strategies

| Name | Class | Type | Direction |
|------|-------|------|-----------|
| `double_top` | `doubleTop` | Pattern | Short |
| `double_bottom` | `doubleBottom` | Pattern | Long |
| `triple_top` | `tripleTop` | Pattern | Short |
| `triple_bottom` | `tripleBottom` | Pattern | Long |
| `head_and_shoulders` | `headAndShoulders` | Pattern | Short |
| `head_and_shoulders_inverse` | `headAndShouldersInverse` | Pattern | Long |
| `buy_and_hold` | `BuyAndHoldStrategy` | Simple | Long |

## Adding Market Data

### CSV File Format

Place CSV files in `backend/data/Historical/Daily/` with naming convention:
`{SYMBOL}_Daily_Bars.csv`

Example: `AAPL_Daily_Bars.csv`

```csv
datetime,Open,High,Low,Close,Volume
2023-01-03,130.28,130.90,124.17,125.07,112117500
2023-01-04,126.89,128.66,125.08,126.36,89113600
```

### Required Columns

- `datetime` - Date (YYYY-MM-DD format)
- `Open` - Opening price
- `High` - High price
- `Low` - Low price
- `Close` - Closing price
- `Volume` - Trading volume

## Development Tasks

### Creating a New Strategy

1. **Create Strategy File**

```python
# src/strategy/strategies/my_strategy.py
from src.strategy.base import Strategy
from src.engine.events import SignalEvent

class MyStrategy(Strategy):
    def __init__(self, bars, events):
        self.bars = bars
        self.events = events
        self.symbol_list = bars.symbol_list
        self.name = "My Strategy"

    def calculate_signals(self, event):
        if event.type == 'MARKET':
            for symbol in self.symbol_list:
                bars = self.bars.get_latest_bars(symbol, N=1)
                # Your logic here
                if should_buy:
                    signal = SignalEvent(
                        symbol=symbol,
                        timestamp=bars.index[0],
                        signal_type='LONG'
                    )
                    self.events.put(signal)
```

2. **Register Strategy**

```python
# src/config.py
from src.strategy.strategies.my_strategy import MyStrategy

STRATEGY_REGISTRY = {
    # ... existing strategies
    "my_strategy": MyStrategy,
}
```

3. **Update Exports**

```python
# src/strategy/__init__.py
from .strategies.my_strategy import MyStrategy
```

### Creating a New Pattern Strategy

1. **Inherit from Patterns Base Class**

```python
# src/strategy/patterns/my_pattern.py
from src.strategy.patterns.base import Patterns

class MyPattern(Patterns):
    def __init__(self, bars, events):
        super().__init__(bars, events)
        self.name = "My Pattern"
        self.datapoints = 3  # Number of points in pattern
        self.bias = "Long"   # or "Short"

    def pattern_scanner(self, minima, maxima, frequency='daily'):
        """Return list of pattern instances found."""
        patterns = []
        # Your pattern detection logic
        return patterns

    def get_PriceData(self, data, pattern_list):
        """Extract price data for found patterns."""
        # Return DataFrame with pattern details
        pass

    def get_ConfDate(self, data, pattern_data):
        """Check for pattern confirmation (breakout)."""
        # Return updated pattern_data with confirmation
        pass

    def risk_Manager(self, pattern_data):
        """Calculate stop-loss and targets."""
        # Add stoploss and target columns
        pass
```

### Creating a New Position Sizer

```python
# src/portfolio/sizing/my_sizer.py
def calculate_position_size(capital, risk_per_trade, stop_distance):
    """
    Calculate position size based on risk parameters.

    Args:
        capital: Available capital
        risk_per_trade: Maximum risk per trade (e.g., 0.02 for 2%)
        stop_distance: Distance to stop-loss in price

    Returns:
        int: Number of shares to trade
    """
    risk_amount = capital * risk_per_trade
    shares = int(risk_amount / stop_distance)
    return shares
```

### Creating a New Broker Handler

```python
# src/broker/my_broker/executor.py
from src.broker.base import ExecutionHandler
from src.engine.events import ExecutionEvent

class MyBrokerExecutionHandler(ExecutionHandler):
    def __init__(self, events, api_key):
        self.events = events
        self.api_key = api_key

    def execute_order(self, event):
        if event.type == 'ORDER':
            # Connect to broker API
            # Submit order
            # Create execution event
            execution = ExecutionEvent(
                order_id=event.order_id,
                status='FILLED',
                symbol=event.symbol,
                direction=event.direction,
                filled_quantity=event.quantity,
                remaining_quantity=0,
                avg_fill_price=actual_price,
                commission=calculated_commission
            )
            self.events.put(execution)
```

## Testing

### Unit Tests

```bash
cd backend
uv run pytest tests/
```

### Integration Tests

```bash
uv run pytest tests/integration/
```

### Test Coverage

```bash
uv run pytest --cov=src tests/
```

## Code Style

### Type Hints

All functions should include type hints:

```python
def calculate_signals(self, event: MarketEvent) -> None:
    pass
```

### Docstrings

Use Google-style docstrings:

```python
def create_sharpe_ratio(returns: pd.Series, periods: int = 252) -> float:
    """
    Calculate the Sharpe ratio for a returns series.

    Args:
        returns: A pandas Series of period percentage returns.
        periods: Number of periods per year (252 for daily).

    Returns:
        The annualized Sharpe ratio.
    """
```

### Naming Conventions

| Type | Convention | Example |
|------|------------|---------|
| Classes | PascalCase | `NaivePortfolio` |
| Functions | snake_case | `calculate_signals` |
| Constants | UPPER_SNAKE | `STRATEGY_REGISTRY` |
| Private | _prefix | `_open_convert_csv_files` |

## Debugging

### Enable Debug Logging

Add to your strategy or component:

```python
from icecream import ic

def calculate_signals(self, event):
    ic(event)
    ic(self.bars.get_latest_bars('AAPL', 5))
```

### Event Queue Inspection

```python
print(f"Queue size: {self.events.qsize()}")
```

### Pattern Visualization

Patterns automatically plot when reaching specific dates (configurable in `patterns/base.py`):

```python
if str(self.latest_symbol_data[s].index[-1]) == "2024-02-08 00:00:00":
    self.plot_min_max(self.latest_symbol_data[s], minima, maxima)
```

## Common Issues

### CSV File Not Found

```
FileNotFoundError: [Errno 2] No such file or directory: '.../AAPL_Daily_Bars.csv'
```

**Solution**: Ensure CSV files follow naming convention `{SYMBOL}_Daily_Bars.csv`

### Invalid Signal Type

```
ValueError: Invalid signal_type: BUY
```

**Solution**: Use `'LONG'`, `'SHORT'`, or `'EXIT'` for signal types

### Empty Queue

```
queue.Empty
```

This is normal - it means all events have been processed for the current bar.
