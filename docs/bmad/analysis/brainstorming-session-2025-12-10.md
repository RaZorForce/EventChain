# Brainstorming Session: EventChainTrader Functional Pillars

**Date:** 2025-12-10
**Facilitator:** BMad Master
**Participant:** Akira
**Techniques Used:** First Principles, Role Playing, Morphological Analysis, Six Thinking Hats

---

## Session Overview

**Topic:** Defining the core functional pillars of EventChainTrader as a complete desktop trading system

**Goals:**
- Identify core functional requirements
- Design concepts for a functional trading system
- Determine technology decisions for implementation

**Context:** Existing Python backtesting engine with Island Architecture, evolving to full desktop trading application for other traders.

---

## First Principles Analysis

### Fundamental Truths Identified

| Truth | Implication |
|-------|-------------|
| Backtest ≠ Live (different data sources, execution) | System must abstract these differences |
| Broker connection is mode-dependent | Clean separation between simulated vs real |
| Trader needs visibility of current mode | UI/UX safety requirement |
| Strategy is user-selectable from library | Strategy as pluggable component |
| Position limits are configurable | Risk management is user-controlled |
| Performance tracking is essential | Both strategy-level and portfolio-level |

---

## 12 Core Pillars

| # | Pillar | Description |
|---|--------|-------------|
| 1 | **Operation Modes** | Backtest / Paper / Live with visible state & seamless switching |
| 2 | **Data Source Management** | yfinance, CSV cache, broker feeds - mode-appropriate |
| 3 | **Broker Integration** | IB first, abstracted interface, auto-reconnect |
| 4 | **Strategy Library** | Patterns, indicators, strategies - user-selectable |
| 5 | **Position Management** | User-configurable limits + manual emergency stop |
| 6 | **Performance Analytics** | Strategy + Portfolio metrics + data export |
| 7 | **Execution Engine** | Simulated ↔ Real routing based on mode |
| 8 | **Alerting & Notifications** | Signals, fills, errors - desktop notifications |
| 9 | **Trade Journal** | Persistent trade history in SQLite |
| 10 | **Settings & Configuration** | App + Account + EOD rules (optional) |
| 11 | **Multi-Timeframe** | 1m, 1h, 1d, 1w bars |
| 12 | **Strategy Optimization** | Grid search, genetic, walk-forward |

---

## Operation Modes

| Mode | Data Source | Execution | Portfolio |
|------|-------------|-----------|-----------|
| **Backtest** | yfinance/Broker (historical → CSV cache) | Simulated | Simulated |
| **Paper** | Broker (streaming) | Real | Real (from broker) |
| **Live** | Broker (streaming) | Real | Real (from broker) |

---

## Mode-Dependency Matrix

| Pillar | Backtest | Paper | Live |
|--------|----------|-------|------|
| **Data Source** | yfinance/Broker (historical → CSV cache) | Broker (streaming) | Broker (streaming) |
| **Execution** | Simulated | Real | Real |
| **Portfolio** | Simulated | Real (from broker) | Real (from broker) |
| **Speed** | As fast as possible | Real-time | Real-time |
| **Reconnection** | Yes (to data source) | Yes | Yes |
| **Emergency Stop** | N/A | Yes | Yes |
| **Journal** | Writes | Writes | Writes |
| **Notifications** | Yes | Yes | Yes |

---

## Role Playing Insights

### Personas Tested

| Persona | Key Needs |
|---------|-----------|
| **Day Trader (Marcus)** | Speed, 1m bars, multi-ticker view, reconnection |
| **Swing Trader (Sarah)** | Alerts, historical review, time-based comparisons |
| **Algo Enthusiast (Alex)** | Multi-timeframe testing, optimization, data export |
| **Beginner (Ben)** | Mode safety, practice path, risk protection |

### Decisions from Role Playing

| Question | Decision |
|----------|----------|
| Connection issues | Attempt auto-reconnection |
| End-of-day automation | User-configurable option, not forced |
| Parameter optimization | Yes, in `strategy/optimizer/` |
| Circuit breaker | Manual emergency stop only - risk is user's responsibility |

---

## Technology Decisions

### UI Framework: Tauri

**Rationale:**
- Lightweight (~5-10MB vs Electron's 150MB+)
- Fast startup (critical for trading)
- Rust backend can call Python engine via sidecar/IPC
- Modern web UI (React/Vue for responsive charts)
- Cross-platform (Windows/macOS/Linux)
- Native system tray for notifications

**Architecture:**
```
┌─────────────────────────────────────────┐
│           Tauri Desktop Shell           │
│  ┌─────────────────────────────────┐    │
│  │   Web UI (React/Vue + Charts)   │    │
│  └─────────────────────────────────┘    │
│                   │ IPC                 │
│  ┌─────────────────────────────────┐    │
│  │   Rust Bridge (lightweight)     │    │
│  └─────────────────────────────────┘    │
│                   │ Subprocess/Socket   │
│  ┌─────────────────────────────────┐    │
│  │   Python Trading Engine         │    │
│  │   (Existing Island Architecture)│    │
│  └─────────────────────────────────┘    │
└─────────────────────────────────────────┘
```

### Journal Storage: SQLite

**Rationale:**
- Single file, portable, easy backup
- Fast queries for historical analysis
- Python native (`sqlite3` built-in)
- Concurrent reads while engine writes

**Draft Schema:**
```sql
CREATE TABLE trades (
    id INTEGER PRIMARY KEY,
    timestamp DATETIME,
    mode TEXT,
    symbol TEXT,
    direction TEXT,
    quantity INTEGER,
    price REAL,
    commission REAL,
    strategy TEXT,
    timeframe TEXT,
    pnl REAL,
    notes TEXT
);

CREATE TABLE sessions (
    id INTEGER PRIMARY KEY,
    start_time DATETIME,
    end_time DATETIME,
    mode TEXT,
    strategy TEXT,
    initial_capital REAL,
    final_capital REAL,
    total_trades INTEGER
);
```

### Notifications: Desktop Only

- System tray notifications via Tauri
- Future consideration: mobile push

---

## Creative Ideas (Six Thinking Hats - Green Hat)

| Idea | Value |
|------|-------|
| "Panic mode" visual theme | When live, UI has distinct color scheme (red border?) |
| Strategy "sandbox" | Test new strategy on subset before full backtest |
| Position heat map | Visual dashboard showing all positions at glance |
| "What-if" mode | After backtest, adjust parameters and see instant impact |

---

## Risks Identified (Six Thinking Hats - Black Hat)

| Risk | Mitigation |
|------|------------|
| User accidentally trades live | Prominent mode indicator, confirmation dialogs |
| Broker disconnect mid-trade | Auto-reconnect + position state recovery |
| Strategy optimization takes forever | Progress indicator, cancellation option |
| Journal grows huge | Archiving, export, cleanup tools |

---

## Summary

### Confirmed Decisions

| Decision | Resolution |
|----------|------------|
| Operation Modes | 3 modes: Backtest, Paper, Live |
| Timeframes | 1m, 1h, 1d, 1w |
| UI Framework | Tauri (web-based, lightweight) |
| Frontend | React or Vue + charting library |
| Journal Storage | SQLite |
| Notifications | Desktop only (system tray) |
| Position Limits | User-configurable |
| Emergency Stop | Manual button, no automated rules |
| Risk Management | User's responsibility (proper order types) |

### Next Steps

1. Create PRD based on these pillars
2. Design architecture incorporating Tauri + Python engine
3. Define epics and stories for implementation
