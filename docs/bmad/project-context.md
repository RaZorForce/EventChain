# Project Context

> Critical rules and patterns for AI agents implementing code in EventChainTrader

## Project Identity

- **Name**: EventChainTrader
- **Type**: Desktop Application - Algorithmic Trading System
- **Architecture**: Event-Driven Island Architecture
- **Language**: Python 3.13+
- **Target Platform**: Desktop (Windows/macOS/Linux)

## Critical Rules

### 1. Event Immutability

**NEVER modify events after creation.** All events use `@dataclass(frozen=True)`.

```python
# CORRECT
signal = SignalEvent(symbol='AAPL', signal_type='LONG', timestamp=now)

# WRONG - Will raise FrozenInstanceError
signal.symbol = 'MSFT'
```

### 2. Island Communication

**Islands communicate ONLY through events.** Never import or call another island directly.

```python
# CORRECT - Use event queue
self.events.put(SignalEvent(...))

# WRONG - Direct coupling
from src.portfolio import NaivePortfolio
portfolio.update_signal(signal)  # Don't do this
```

### 3. Signal Types

Valid signal types are: `'LONG'`, `'SHORT'`, `'EXIT'`

```python
# CORRECT
SignalEvent(signal_type='LONG', ...)

# WRONG - Will raise ValueError
SignalEvent(signal_type='BUY', ...)
```

### 4. Order Directions

Valid order directions are: `'BUY'`, `'SELL'`

```python
# CORRECT
OrderEvent(direction='BUY', ...)

# WRONG
OrderEvent(direction='LONG', ...)
```

### 5. Execution Statuses

Valid statuses: `'PARTIAL'`, `'FILLED'`, `'REJECTED'`, `'CANCELLED'`

### 6. Event Queue Pattern

Always use non-blocking get in the event loop:

```python
# CORRECT
event = self.events.get(False)  # Non-blocking

# WRONG - Will block forever if queue is empty
event = self.events.get()
```

### 7. Pattern Strategy State Machine

Pattern strategies MUST follow state machine:
```
SCANNING → CONFIRMING → BUYING
```

Never skip states or generate signals in SCANNING state.

### 8. Data Handler Interface

All data handlers MUST implement:
- `get_latest_bars(symbol, N)` - Return last N bars
- `update_bars()` - Push next bar and emit MarketEvent

### 9. Strategy Interface

All strategies MUST implement:
- `calculate_signals(event)` - React to MarketEvent

### 10. Portfolio Interface

All portfolios MUST implement:
- `update_signal(event)` - Convert SignalEvent to OrderEvent
- `update_fill(event)` - Update positions from ExecutionEvent

### 11. Broker Interface

All brokers MUST implement:
- `execute_order(event)` - Execute OrderEvent, emit ExecutionEvent

## File Naming Conventions

| Type | Pattern | Example |
|------|---------|---------|
| CSV Data | `{SYMBOL}_Daily_Bars.csv` | `AAPL_Daily_Bars.csv` |
| Config | `{mode}.yaml` | `backtest.yaml` |
| Strategy | `{pattern_name}.py` | `double_top.py` |

## Configuration Priority

1. Environment variable `CONFIG_NAME`
2. Default: `"backtest"`

## Import Order

```python
# Standard library
import os
from queue import Queue

# Third-party
import numpy as np
import pandas as pd

# Local - events first
from src.engine.events import SignalEvent, MarketEvent

# Local - base classes
from src.strategy.base import Strategy

# Local - implementations
from src.data_handler import HistoricCSVDataHandler
```

## Key Constants

```python
# Signal types
SIGNAL_LONG = 'LONG'
SIGNAL_SHORT = 'SHORT'
SIGNAL_EXIT = 'EXIT'

# Order directions
ORDER_BUY = 'BUY'
ORDER_SELL = 'SELL'

# Order types
ORDER_MARKET = 'MKT'
ORDER_LIMIT = 'LMT'
ORDER_STOP = 'STP'

# Execution statuses
EXEC_PARTIAL = 'PARTIAL'
EXEC_FILLED = 'FILLED'
EXEC_REJECTED = 'REJECTED'
EXEC_CANCELLED = 'CANCELLED'
```

## Testing Patterns

### Event Creation

```python
def test_signal_event():
    signal = SignalEvent(
        symbol='AAPL',
        signal_type='LONG',
        strength=1.0
    )
    assert signal.type == 'SIGNAL'
    assert signal.symbol == 'AAPL'
```

### Strategy Testing

```python
def test_strategy_signals():
    events = Queue()
    bars = MockDataHandler(events, [...])
    strategy = MyStrategy(bars, events)

    # Emit market event
    events.put(MarketEvent())

    # Process
    event = events.get()
    strategy.calculate_signals(event)

    # Check for signal
    if not events.empty():
        signal = events.get()
        assert signal.type == 'SIGNAL'
```

## Performance Considerations

1. **Avoid deep copies** - Events are frozen, no need to copy
2. **Use iterators** - DataHandler uses generators for memory efficiency
3. **Batch holdings updates** - Update at end of bar, not per-event

## Common Pitfalls

| Issue | Wrong | Correct |
|-------|-------|---------|
| Modifying event | `event.symbol = 'X'` | Create new event |
| Signal type | `'BUY'` | `'LONG'` |
| Order direction | `'LONG'` | `'BUY'` |
| Blocking queue | `events.get()` | `events.get(False)` |
| Direct coupling | `portfolio.update()` | `events.put(OrderEvent)` |

## Extension Checklist

When adding new components:

- [ ] Implement required abstract methods
- [ ] Use frozen dataclasses for any new events
- [ ] Register in appropriate `__init__.py`
- [ ] Update `__all__` exports
- [ ] Add to STRATEGY_REGISTRY if strategy
- [ ] Follow existing naming conventions
- [ ] Add type hints
- [ ] Write docstrings
