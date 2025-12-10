# EventChainTrader Documentation Index

> Primary entry point for AI-assisted development

## Project Overview

| Attribute | Value |
|-----------|-------|
| **Project** | EventChainTrader |
| **Type** | Desktop Application - Algorithmic Trading System |
| **Repository** | Monolith |
| **Language** | Python 3.13+ |
| **Architecture** | Event-Driven Island Architecture |
| **Package Manager** | uv |
| **Target Platform** | Desktop (Windows/macOS/Linux) |

## Quick Reference

### Technology Stack

| Category | Technology |
|----------|------------|
| Core | Python 3.13+, numpy, pandas |
| Signal Processing | scipy, peakutils |
| Visualization | matplotlib, mplfinance |
| Configuration | pyyaml |

### Architecture Pattern

```
DataHandler ─[Market]─▶ Strategy ─[Signal]─▶ Portfolio ─[Order]─▶ Broker
                                                 ▲                    │
                                                 └────[Execution]─────┘
```

### Islands (Domains)

| Island | Responsibility | Key File |
|--------|---------------|----------|
| `data_handler/` | Market data feeds | `csv/reader.py` |
| `strategy/` | Trading signal generation | `patterns/base.py` |
| `portfolio/` | Position & risk management | `portfolios/naive.py` |
| `broker/` | Order execution | `simulated/executor.py` |
| `engine/` | Event orchestration | `trading.py` |

### Event Types

| Event | Producer | Purpose |
|-------|----------|---------|
| `MarketEvent` | DataHandler | New bar available |
| `SignalEvent` | Strategy | Trading opportunity |
| `OrderEvent` | Portfolio | Execution request |
| `ExecutionEvent` | Broker | Fill result |

### Available Strategies

| Strategy | Pattern | Direction |
|----------|---------|-----------|
| `double_top` | Double Top | Short |
| `double_bottom` | Double Bottom | Long |
| `head_and_shoulders` | H&S | Short |
| `buy_and_hold` | N/A | Long |

## Generated Documentation

### Core Documents

- [Architecture](./architecture.md) - System design and component details
- [Source Tree Analysis](./source-tree-analysis.md) - Complete folder structure
- [Data Models](./data-models.md) - Events, positions, configurations
- [Development Guide](./development-guide.md) - Setup, running, extending
- [Project Context](./project-context.md) - Rules for AI agents

### Reference Data

- [Project Scan Report](./project-scan-report.json) - Workflow state file

## Existing Documentation

From original project:

- [App Architecture Design History](../app-arch-design-conversation-history.md) - Island architecture refactor conversation
- [Strategy Island Design History](../strategy-island-design-conversation-history.md) - Strategy design conversation
- [Backend README](../../backend/README.md) - Original backend documentation
- [Source README](../../backend/src/README.md) - Event-driven system overview
- [Engine README](../../backend/src/engine/README.md) - Engine documentation

## Getting Started

### Run a Backtest

```bash
cd backend
uv run python -m src.main
```

### Change Strategy

Edit `backend/config/backtest.yaml`:

```yaml
strategy: double_bottom  # or: triple_top, head_and_shoulders, etc.
symbols:
  - AAPL
  - MSFT
```

### Add Market Data

Place CSV files in `backend/data/Historical/Daily/`:
- Format: `{SYMBOL}_Daily_Bars.csv`
- Columns: datetime, Open, High, Low, Close, Volume

## For AI Agents

When implementing new features or fixing bugs:

1. **Read First**: [Project Context](./project-context.md) - Critical rules
2. **Architecture**: [Architecture](./architecture.md) - System design
3. **Extension**: [Development Guide](./development-guide.md) - How to extend

### Key Rules Summary

1. Events are **immutable** (`frozen=True`)
2. Islands communicate **only through events**
3. Signal types: `'LONG'`, `'SHORT'`, `'EXIT'`
4. Order directions: `'BUY'`, `'SELL'`
5. Use non-blocking queue get: `events.get(False)`

## Documentation Metadata

| Field | Value |
|-------|-------|
| Generated | 2025-12-10 |
| Scan Level | Exhaustive |
| Files Analyzed | ~110 |
| Workflow Version | 1.2.0 |
