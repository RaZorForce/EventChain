# Data Models

## Overview

EventChainTrader uses a combination of pandas DataFrames, Python dataclasses, and dictionaries for data representation. The system is designed around immutable events and mutable state containers.

## Event Data Models

All events are **immutable** Python dataclasses (`@dataclass(frozen=True)`) to prevent bugs and enable event replay.

### Base Event

```python
@dataclass(frozen=True)
class Event:
    version: str = "1.0"  # For backward compatibility
```

### MarketEvent

Emitted when new market data is available.

```python
@dataclass(frozen=True)
class MarketEvent(Event):
    type: str = 'MARKET'
```

### SignalEvent

Emitted when a trading opportunity is detected.

```python
@dataclass(frozen=True)
class SignalEvent(Event):
    type: str = 'SIGNAL'
    strategy_id: str = ''
    symbol: str = ''
    signal_type: str = ''      # 'LONG', 'SHORT', 'EXIT'
    strength: float = 1.0      # Signal confidence (0.0-1.0)
    timestamp: datetime = None
    metadata: Dict[str, Any] = field(default_factory=dict)
```

### OrderEvent

Emitted to request trade execution.

```python
@dataclass(frozen=True)
class OrderEvent(Event):
    type: str = 'ORDER'
    order_id: str = ''
    symbol: str = ''
    order_type: str = 'MKT'    # 'MKT', 'LMT', 'STP'
    direction: str = ''        # 'BUY', 'SELL'
    quantity: int = 0
    price: Optional[float] = None      # For limit orders
    stop_price: Optional[float] = None # For stop orders
    time_in_force: str = 'DAY'         # 'DAY', 'GTC', 'IOC'
    timestamp: datetime = None
```

### ExecutionEvent

Emitted when order execution status changes.

```python
@dataclass(frozen=True)
class ExecutionEvent(Event):
    type: str = 'EXECUTION'
    order_id: str = ''
    status: str = ''           # 'PARTIAL', 'FILLED', 'REJECTED', 'CANCELLED'
    symbol: str = ''
    direction: str = ''        # 'BUY', 'SELL'
    filled_quantity: int = 0   # Cumulative filled
    remaining_quantity: int = 0
    avg_fill_price: float = 0.0
    commission: float = 0.0
    timestamp: datetime = None
    reason: str = ''           # For rejections/cancellations
```

### PortfolioStateEvent

Emitted when portfolio state changes.

```python
@dataclass(frozen=True)
class PortfolioStateEvent(Event):
    type: str = 'PORTFOLIO_STATE'
    # Additional fields defined in portfolio.py
```

### SystemEvent

Emitted for system control.

```python
@dataclass(frozen=True)
class SystemEvent(Event):
    type: str = 'SYSTEM'
    # Additional fields defined in system.py
```

## Market Data Models

### Bar Data (OHLCV)

Market data is stored in pandas DataFrames with datetime index:

| Column | Type | Description |
|--------|------|-------------|
| `Open` | float | Opening price |
| `High` | float | Highest price |
| `Low` | float | Lowest price |
| `Close` | float | Closing price |
| `Volume` | int | Trading volume |

**Index**: `datetime` (pandas DatetimeIndex)

### CSV File Format

Expected format for `{SYMBOL}_Daily_Bars.csv`:

```csv
datetime,Open,High,Low,Close,Volume
2023-01-03,100.00,102.50,99.50,101.25,1000000
2023-01-04,101.25,103.00,100.00,102.75,950000
```

### latest_symbol_data

Per-symbol DataFrame accumulating bars during backtest:

```python
latest_symbol_data = {
    "SYMBOL": pd.DataFrame  # Growing DataFrame of all bars seen
}
```

## Portfolio Data Models

### Position Tracking

**current_positions** - Quantity held per symbol:

```python
current_positions = {
    "AAPL": 100,      # Long 100 shares
    "MSFT": -50,      # Short 50 shares
    "GOOGL": 0        # No position
}
```

**all_positions** - Historical position snapshots:

```python
all_positions = [
    {"datetime": "2023-01-03", "AAPL": 0, "MSFT": 0},
    {"datetime": "2023-01-04", "AAPL": 100, "MSFT": 0},
    # ...
]
```

### Holdings Tracking

**current_holdings** - Market value and cash:

```python
current_holdings = {
    "AAPL": 10125.00,      # Market value of AAPL position
    "MSFT": -5137.50,      # Market value of MSFT short
    "cash": 84500.00,      # Available cash
    "commission": 14.00,   # Cumulative commission paid
    "total": 89501.50      # Total portfolio value
}
```

**all_holdings** - Historical holdings snapshots:

```python
all_holdings = [
    {"datetime": "2023-01-03", "AAPL": 0.0, "cash": 100000.0, "commission": 0.0, "total": 100000.0},
    {"datetime": "2023-01-04", "AAPL": 10125.0, "cash": 89861.0, "commission": 14.0, "total": 100000.0},
    # ...
]
```

### Equity Curve DataFrame

Generated after backtest completion:

| Column | Type | Description |
|--------|------|-------------|
| `datetime` | datetime | Index |
| `{SYMBOL}` | float | Market value per symbol |
| `cash` | float | Cash balance |
| `commission` | float | Cumulative commission |
| `total` | float | Total portfolio value |
| `returns` | float | Period percentage return |
| `equity_curve` | float | Cumulative return factor |

## Pattern Data Models

### Pattern Detection DataFrame

Created by pattern strategies when a pattern is detected:

**Double Top Example**:

| Column | Type | Description |
|--------|------|-------------|
| `top1_date` | datetime | Date of first peak |
| `neck1_date` | datetime | Date of valley (neckline) |
| `top2_date` | datetime | Date of second peak |
| `top1_price` | float | Price at first peak |
| `neck1_price` | float | Price at valley |
| `top2_price` | float | Price at second peak |
| `is_detected` | bool | Pattern detected flag |
| `is_confirmed` | bool | Breakout confirmed |
| `is_bought` | bool | Signal sent |
| `confirmation_date` | datetime | Date of confirmation |
| `signal` | int | -1 (short) or 1 (long) |
| `time_for_confirmation` | int | Days to confirm |
| `stoploss` | float | Calculated stop-loss |
| `target` | float | Price target |
| `top_length` | float | Distance head to neckline |

### Pattern State Machine

```python
pattern_state = {
    "AAPL": "SCANNING",     # Looking for pattern
    "MSFT": "CONFIRMING",   # Pattern found, waiting for breakout
    "GOOGL": "BUYING"       # Confirmed, ready to signal
}
```

**State Transitions**:
```
SCANNING ──[pattern found]──▶ CONFIRMING ──[breakout]──▶ BUYING ──[signal sent]──▶ (end)
```

## Configuration Data Models

### Backtest Configuration (YAML)

```yaml
strategy: string          # e.g., "double_top"
symbols:                  # List of tickers
  - AAPL
  - MSFT
data:
  csv_dir: string         # e.g., "data/Historical/Daily"
  start_date: string      # e.g., "20230215"
portfolio:
  initial_capital: float  # e.g., 100000.0
```

### Event Bus Configuration (YAML)

```yaml
events:
  - name: string
    description: string
    provider: string
    consumers: list[string]
    priority: int

islands:
  island_name:
    produces: list[string]
    consumes: list[string]
    description: string

routing:
  synchronous_events: list[string]
  asynchronous_events: list[string]
  queue:
    type: string           # "priority", "fifo", "lifo"
    max_size: int
    overflow_strategy: string
```

## Strategy Registry

Maps strategy names to classes:

```python
STRATEGY_REGISTRY = {
    "double_top": doubleTop,
    "double_bottom": doubleBottom,
    "triple_top": tripleTop,
    "triple_bottom": tripleBottom,
    "head_and_shoulders": headAndShoulders,
    "head_and_shoulders_inverse": headAndShouldersInverse,
    "buy_and_hold": BuyAndHoldStrategy,
}
```

## Performance Metrics

### Output Summary Stats

```python
stats = [
    ("Total Return", "12.34%"),
    ("Sharpe Ratio", "1.23"),
    ("Max Drawdown", "5.67%"),
    ("Drawdown Duration", "45")  # Days
]
```

### Sharpe Ratio Calculation

```python
sharpe = sqrt(periods) * mean(returns) / std(returns)
# periods = 252 for daily data
```

### Drawdown Calculation

- **Max Drawdown**: Largest peak-to-trough decline
- **Drawdown Duration**: Longest recovery period (in bars)
