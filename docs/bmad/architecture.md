# EventChainTrader Architecture

## Executive Summary

EventChainTrader is a **desktop application** for algorithmic trading, designed for backtesting and live trading. The system processes market data bar-by-bar, detects trading patterns, generates orders, and simulates (or executes) trades with configurable commission and slippage models.

**Target Platform**: Desktop (Windows/macOS/Linux)

## Technology Stack

| Category | Technology | Version | Purpose |
|----------|------------|---------|---------|
| Language | Python | 3.13+ | Core implementation |
| Package Manager | uv | Latest | Dependency management |
| Data Processing | numpy | 2.3.5+ | Numerical computations |
| Data Processing | pandas | 2.3.3+ | Time series & DataFrames |
| Signal Processing | scipy | 1.16.3+ | Peak detection, optimization |
| Visualization | matplotlib | 3.10.7+ | Charts and plots |
| Visualization | mplfinance | 0.12.10b0+ | Candlestick charts |
| Utilities | peakutils | 1.3.5+ | Peak detection |
| Configuration | pyyaml | 6.0+ | YAML config parsing |

## Architecture Pattern: Event-Driven Island Architecture

The system implements an **Island Architecture** where five independent domains (islands) communicate exclusively through an event queue. This provides:

- **Loose Coupling**: Components only know about events, not each other
- **Testability**: Each island can be tested in isolation
- **Flexibility**: Swap backtesting for live trading by changing only the DataHandler
- **Extensibility**: Add new strategies or brokers without modifying core logic

### Event Flow

```
DataHandler ──[MarketEvent]──▶ Strategy ──[SignalEvent]──▶ Portfolio ──[OrderEvent]──▶ Broker
                                                              ▲                           │
                                                              └──────[ExecutionEvent]─────┘
```

## Island Definitions

### 1. Data Handler Island (`src/data_handler/`)

**Responsibility**: Provide market data to the system

| Aspect | Details |
|--------|---------|
| **Produces** | `MarketEvent` |
| **Consumes** | `SystemEvent` |
| **Key Classes** | `DataHandler` (ABC), `HistoricCSVDataHandler` |
| **Pattern** | Iterator pattern for bar-by-bar data streaming |

**Submodules**:
- `csv/` - CSV file reading and parsing
- `cache/` - Memory and disk caching
- `transformers/` - Data normalization, adjustment, resampling
- `live/` - Live data feed handlers (extensibility)

### 2. Strategy Island (`src/strategy/`)

**Responsibility**: Generate trading signals from market data

| Aspect | Details |
|--------|---------|
| **Produces** | `SignalEvent` |
| **Consumes** | `MarketEvent`, `PortfolioStateEvent`, `SystemEvent` |
| **Key Classes** | `Strategy` (ABC), `Patterns` (pattern base), concrete patterns |
| **Pattern** | State machine (SCANNING → CONFIRMING → BUYING) |

**Available Strategies**:
- Chart Patterns: `doubleTop`, `doubleBottom`, `tripleTop`, `tripleBottom`, `headAndShoulders`, `headAndShouldersInverse`
- Simple: `BuyAndHoldStrategy`

**Pattern Recognition Process**:
1. Detect peaks and valleys using `scipy.signal.find_peaks`
2. Scan for pattern formation (e.g., two peaks for double top)
3. Wait for confirmation (breakout below/above neckline)
4. Generate signal with calculated stop-loss and targets

### 3. Portfolio Island (`src/portfolio/`)

**Responsibility**: Manage positions, risk, and generate orders

| Aspect | Details |
|--------|---------|
| **Produces** | `OrderEvent`, `PortfolioStateEvent` |
| **Consumes** | `MarketEvent`, `SignalEvent`, `ExecutionEvent`, `SystemEvent` |
| **Key Classes** | `Portfolio` (ABC), `NaivePortfolio` |
| **Pattern** | Position tracking with holdings valuation |

**Submodules**:
- `allocation/` - Portfolio allocation optimization
- `metrics/` - Sharpe ratio, drawdowns
- `positions/` - Position tracking
- `risk/` - Risk management
- `sizing/` - Position sizing (fixed, percent, Kelly, risk parity)

### 4. Broker Island (`src/broker/`)

**Responsibility**: Execute orders and report fills

| Aspect | Details |
|--------|---------|
| **Produces** | `ExecutionEvent` |
| **Consumes** | `OrderEvent`, `SystemEvent` |
| **Key Classes** | `ExecutionHandler` (ABC), `SimulatedExecutionHandler` |
| **Pattern** | Command pattern for order execution |

**Submodules**:
- `simulated/` - Backtest execution with commission/slippage models
- `interactive_brokers/` - Real broker integration
- `execution_algos/` - TWAP, VWAP, Iceberg algorithms
- `order_management/` - Order lifecycle tracking

### 5. Engine Island (`src/engine/`)

**Responsibility**: Orchestrate event flow and system lifecycle

| Aspect | Details |
|--------|---------|
| **Produces** | `SystemEvent` |
| **Consumes** | `PortfolioStateEvent` |
| **Key Classes** | `TradingEngine` |
| **Pattern** | Event loop with priority queue |

**Submodules**:
- `events/` - All event type definitions
- `event_loop/` - Event processing, routing, priority
- `monitoring/` - Health checks, metrics, alerting
- `persistence/` - Event logging, snapshots, state management
- `scheduler/` - Cron, timers, triggers

## Event Types

All events inherit from immutable `Event` base class (`@dataclass(frozen=True)`):

| Event | Producer | Consumer(s) | Purpose |
|-------|----------|-------------|---------|
| `MarketEvent` | DataHandler | Strategy, Portfolio | New bar available |
| `SignalEvent` | Strategy | Portfolio | Trading opportunity detected |
| `OrderEvent` | Portfolio | Broker | Order execution request |
| `ExecutionEvent` | Broker | Portfolio | Order fill/rejection result |
| `PortfolioStateEvent` | Portfolio | Strategy, Engine | Portfolio state update |
| `SystemEvent` | Engine | All | System control (start/stop) |

### Event Priority (from event_bus.yaml)

```
0: SYSTEM (Critical - processed first)
1: MARKET (Highest data priority)
2: SIGNAL
3: ORDER
4: EXECUTION
5: PORTFOLIO_STATE
```

## Data Models

### Core Data Structures

**Bar Data** (OHLCV):
```python
{
    "Date": datetime,      # Index
    "Open": float,
    "High": float,
    "Low": float,
    "Close": float,
    "Volume": int
}
```

**Position Dictionary**:
```python
current_positions = {
    "SYMBOL": quantity,     # int, negative = short
    "datetime": timestamp
}
```

**Holdings Dictionary**:
```python
current_holdings = {
    "SYMBOL": market_value,  # float
    "cash": float,
    "commission": float,
    "total": float
}
```

### Configuration Schema (backtest.yaml)

```yaml
strategy: string          # Strategy name from registry
symbols: list[string]     # Ticker symbols to trade
data:
  csv_dir: string         # Path to CSV data
  start_date: string      # YYYYMMDD format
portfolio:
  initial_capital: float  # Starting capital
```

## Main Event Loop

```python
while data_handler.continue_backtest:     # Outer loop (heartbeat)
    data_handler.update_bars()            # Push new bar, emit MarketEvent

    while True:                            # Inner loop (process all events)
        event = events.get(False)          # Non-blocking get
        if event.type == 'MARKET':
            strategy.calculate_signals(event)
            portfolio.update_timeindex(event)
        elif event.type == 'SIGNAL':
            portfolio.update_signal(event)
        elif event.type == 'ORDER':
            broker.execute_order(event)
        elif event.type == 'EXECUTION':
            portfolio.update_fill(event)
```

## Extension Points

| Extension Point | How to Extend |
|-----------------|---------------|
| New Data Source | Implement `DataHandler` interface |
| New Strategy | Inherit from `Strategy` or `Patterns` |
| New Position Sizing | Add to `portfolio/sizing/` |
| New Broker | Implement `ExecutionHandler` interface |
| New Execution Algo | Add to `broker/execution_algos/` |

## Development Notes

### Running Backtests

```bash
cd backend
uv run python -m src.main
```

### Configuration

Edit `config/backtest.yaml` to change:
- Strategy selection
- Symbol list
- Data source path
- Initial capital

### Environment Variables

- `CONFIG_NAME` - Override default config file (default: "backtest")
