# Source Tree Analysis

## Project Structure Overview

EventChainTrader is an **event-driven algorithmic trading system** implemented as a Python monolith using an **Island Architecture** pattern. The system is organized into five islands (domains), each responsible for a specific concern.

```
EventChainTrader/
├── backend/                          # Main application root
│   ├── config/                       # YAML configuration files
│   │   ├── backtest.yaml            # Backtest mode configuration
│   │   ├── event_bus.yaml           # Event routing & island definitions
│   │   ├── forwardtest.yaml         # Forward test configuration
│   │   ├── live.yaml                # Live trading configuration
│   │   └── paper.yaml               # Paper trading configuration
│   │
│   ├── data/                         # Historical market data (CSV files)
│   │   └── Historical/Daily/        # Daily OHLCV bar data
│   │
│   └── src/                          # Source code root
│       ├── __init__.py              # Public API exports
│       ├── config.py                # Configuration loader & strategy registry
│       ├── main.py                  # Application entry point
│       │
│       ├── data_handler/            # ISLAND: Market Data Management
│       │   ├── __init__.py
│       │   ├── base.py              # Abstract DataHandler interface
│       │   ├── cache/               # Data caching strategies
│       │   │   ├── disk_cache.py
│       │   │   └── memory_cache.py
│       │   ├── csv/                 # CSV file handling
│       │   │   ├── parser.py
│       │   │   ├── reader.py        # HistoricCSVDataHandler implementation
│       │   │   └── validator.py
│       │   ├── live/                # Live data feed handlers (placeholder)
│       │   │   └── __init__.py
│       │   └── transformers/        # Data transformation utilities
│       │       ├── adjuster.py
│       │       ├── normalizer.py
│       │       └── resampler.py
│       │
│       ├── strategy/                # ISLAND: Trading Strategy Logic
│       │   ├── __init__.py
│       │   ├── base.py              # Abstract Strategy interface
│       │   ├── indicators/          # Technical indicators
│       │   │   ├── moving_average.py
│       │   │   ├── oscillators.py
│       │   │   ├── volatility.py
│       │   │   └── volume.py
│       │   ├── optimizer/           # Strategy optimization
│       │   │   ├── genetic.py       # Genetic algorithm optimizer
│       │   │   ├── grid_search.py   # Grid search optimizer
│       │   │   └── walk_forward.py  # Walk-forward analysis
│       │   ├── patterns/            # Chart pattern recognition
│       │   │   ├── base.py          # Patterns base class (state machine)
│       │   │   ├── double_bottom.py
│       │   │   ├── double_top.py
│       │   │   ├── head_and_shoulders.py
│       │   │   ├── head_and_shoulders_inverse.py
│       │   │   ├── triple_bottom.py
│       │   │   └── triple_top.py
│       │   └── strategies/          # Complete strategy implementations
│       │       └── buy_and_hold.py
│       │
│       ├── portfolio/               # ISLAND: Position & Risk Management
│       │   ├── __init__.py
│       │   ├── base.py              # Abstract Portfolio interface
│       │   ├── allocation/          # Portfolio allocation strategies
│       │   │   ├── allocator.py
│       │   │   ├── optimizer.py
│       │   │   └── rebalancer.py
│       │   ├── metrics/             # Performance metrics
│       │   │   └── performance.py   # Sharpe ratio, drawdowns
│       │   ├── portfolios/          # Portfolio implementations
│       │   │   └── naive.py         # NaivePortfolio implementation
│       │   ├── positions/           # Position tracking
│       │   │   └── tracker.py
│       │   ├── risk/                # Risk management
│       │   │   └── manager.py
│       │   └── sizing/              # Position sizing strategies
│       │       ├── fixed.py
│       │       ├── kelly.py         # Kelly criterion
│       │       ├── percent.py
│       │       └── risk_parity.py
│       │
│       ├── broker/                  # ISLAND: Order Execution
│       │   ├── __init__.py
│       │   ├── base.py              # Abstract ExecutionHandler interface
│       │   ├── execution_algos/     # Advanced execution algorithms
│       │   │   ├── iceberg.py       # Iceberg order splitting
│       │   │   ├── twap.py          # Time-weighted average price
│       │   │   └── vwap.py          # Volume-weighted average price
│       │   ├── interactive_brokers/ # IB integration
│       │   │   ├── __init__.py
│       │   │   └── executor.py
│       │   ├── interactive_brokers.py  # IB handler (legacy)
│       │   ├── order_management/    # Order lifecycle
│       │   │   ├── order.py
│       │   │   ├── tracker.py
│       │   │   └── validator.py
│       │   └── simulated/           # Simulated broker
│       │       ├── commission.py    # Commission models
│       │       ├── executor.py      # SimulatedExecutionHandler
│       │       └── slippage.py      # Slippage models
│       │
│       └── engine/                  # ISLAND: Core Event Processing
│           ├── __init__.py
│           ├── trading.py           # TradingEngine - main orchestrator
│           ├── event_bus_config.py  # Event bus configuration loader
│           ├── engines/             # Engine implementations
│           │   └── backtest.py      # Backtest-specific engine logic
│           ├── event_loop/          # Event processing infrastructure
│           │   ├── priority.py      # Priority queue implementation
│           │   ├── processor.py     # Event processor
│           │   └── router.py        # Event routing logic
│           ├── events/              # Event type definitions
│           │   ├── __init__.py      # Event exports
│           │   ├── base.py          # Base Event class (immutable)
│           │   ├── execution.py     # ExecutionEvent
│           │   ├── market.py        # MarketEvent
│           │   ├── order.py         # OrderEvent
│           │   ├── portfolio.py     # PortfolioStateEvent
│           │   ├── signal.py        # SignalEvent
│           │   └── system.py        # SystemEvent
│           ├── monitoring/          # System monitoring
│           │   ├── alerting.py
│           │   ├── health_check.py
│           │   └── metrics.py
│           ├── persistence/         # State persistence
│           │   ├── event_log.py
│           │   ├── snapshot.py
│           │   └── state_manager.py
│           └── scheduler/           # Task scheduling
│               ├── cron.py
│               ├── timer.py
│               └── triggers.py
│
└── docs/                            # Documentation
    ├── app-arch-design-conversation-history.md
    ├── strategy-island-design-conversation-history.md
    └── bmad/                        # BMad-generated documentation
```

## Critical Entry Points

| File | Purpose |
|------|---------|
| `backend/src/main.py` | Application entry point - initializes all components and runs engine |
| `backend/src/config.py` | Configuration loader and strategy registry |
| `backend/config/backtest.yaml` | Primary configuration file |

## Island Architecture

The codebase follows an **Island Architecture** where each island (domain) communicates only through events:

```
┌─────────────────────────────────────────────────────────────────────┐
│                          ENGINE (Orchestrator)                       │
│                    Produces: SystemEvent                             │
│                    Consumes: PortfolioStateEvent                     │
└─────────────────────────────────────────────────────────────────────┘
                                    │
        ┌───────────────────────────┼───────────────────────────┐
        ▼                           ▼                           ▼
┌───────────────┐           ┌───────────────┐           ┌───────────────┐
│  DATA_HANDLER │──Market──▶│   STRATEGY    │──Signal──▶│   PORTFOLIO   │
│               │   Event   │               │   Event   │               │
│ Produces:     │           │ Produces:     │           │ Produces:     │
│ - MarketEvent │           │ - SignalEvent │           │ - OrderEvent  │
│               │           │               │           │ - PortfolioState│
│ Consumes:     │           │ Consumes:     │           │               │
│ - SystemEvent │           │ - MarketEvent │           │ Consumes:     │
└───────────────┘           │ - Portfolio   │           │ - MarketEvent │
                            │   StateEvent  │           │ - SignalEvent │
                            │ - SystemEvent │           │ - Execution   │
                            └───────────────┘           │   Event       │
                                                        │ - SystemEvent │
                                                        └───────┬───────┘
                                                                │
                                                          Order │
                                                          Event │
                                                                ▼
                                                        ┌───────────────┐
                                                        │    BROKER     │
                                                        │               │
                                                        │ Produces:     │
                                                        │ - Execution   │
                                                        │   Event       │
                                                        │               │
                                                        │ Consumes:     │
                                                        │ - OrderEvent  │
                                                        │ - SystemEvent │
                                                        └───────────────┘
```

## File Statistics

| Category | Count |
|----------|-------|
| Total Python Files | ~110 |
| Configuration Files | 5 YAML |
| Islands | 5 |
| Event Types | 6 |
| Pattern Strategies | 7 |
| Technical Indicators | 4 categories |
| Position Sizing Methods | 4 |
| Execution Algorithms | 3 |
