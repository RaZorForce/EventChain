# -*- coding: utf-8 -*-

import json
import logging
from datetime import datetime
from queue import Queue
from typing import Dict, Optional

from src.data_handler import DataHandler
from src.engine.events import (
    ExecutionEvent,
    MarketEvent,
    OrderEvent,
    PortfolioStateEvent,
    SignalEvent,
)
from src.portfolio.base import Portfolio
from src.portfolio.sizing.fixed import FixedSizer


class BasicPortfolio(Portfolio):
    """
    A robust, event-driven Portfolio implementation.

    Features:
    - Immutable PortfolioStateEvent emission on every state change.
    - Distinct tracking of positions, cash, Realized P&L, and Unrealized P&L.
    - Weighted Average Cost basis for P&L calculations.
    - Automatic cleanup of zero-quantity positions.
    - Structured logging.
    - Integration with FixedSizer for basic order sizing.
    """

    def __init__(
        self,
        bars: DataHandler,
        events: Queue,
        initial_capital: float = 100000.0,
        sizer=None,
    ):
        """
        Initialize the BasicPortfolio.

        Args:
            bars: DataHandler for market data access.
            events: Event queue for publishing Order/Portfolio events.
            initial_capital: Starting cash.
            sizer: Position sizer instance (default: FixedSizer).
        """
        self.bars = bars
        self.events = events
        self.initial_capital = initial_capital
        
        # State
        self.current_positions: Dict[str, int] = {}  # Symbol -> Quantity
        self.avg_cost: Dict[str, float] = {}  # Symbol -> Weighted Avg Cost Price
        
        self.cash = initial_capital
        self.realized_pnl = 0.0
        self.unrealized_pnl = 0.0
        self.total_equity = initial_capital

        # Components
        self.sizer = sizer if sizer else FixedSizer()

        # Logging
        self.logger = logging.getLogger("Portfolio")
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            ch = logging.StreamHandler()
            ch.setFormatter(logging.Formatter('%(message)s'))
            self.logger.addHandler(ch)

        self._log("INIT", {"initial_capital": initial_capital})
        self._emit_state()

    def update_signal(self, event: SignalEvent) -> None:
        """
        React to SignalEvent: generate an Order using the Sizer.
        """
        if event.type != 'SIGNAL':
            return

        order = self._generate_order(event)
        if order:
            self.events.put(order)
            self._log("ORDER_GENERATED", {"symbol": order.symbol, "quantity": order.quantity, "direction": order.direction})

    def update_fill(self, event: ExecutionEvent) -> None:
        """
        React to ExecutionEvent (Fill): update positions, cash, and Realized P&L.
        """
        if event.type != 'EXECUTION':
            return

        self._process_fill(event)
        self._update_unrealized_pnl() # Re-calc unrealized based on new state/market
        self._update_equity()
        self._emit_state()

    def update_market(self, event: MarketEvent) -> None:
        """
        React to MarketEvent: update Unrealized P&L and Equity (Mark-to-Market).
        """
        if event.type != 'MARKET':
            return

        self._update_unrealized_pnl()
        self._update_equity()
        # We might not want to emit full state on EVERY tick if high freq, 
        # but for this basic implementation, we will for transparency.
        self._emit_state()

    def update_timeindex(self, event: MarketEvent) -> None:
        """
        Alias for update_market to satisfy the Engine interface.
        """
        self.update_market(event)

    def _generate_order(self, signal: SignalEvent) -> Optional[OrderEvent]:
        """
        Internal: Create an OrderEvent using the Sizer.
        """
        quantity = self.sizer.calculate_quantity(signal.strength)
        if quantity <= 0:
            return None

        # Basic logic: 
        # LONG -> Buy to open or Sell to close
        # SHORT -> Sell to open or Buy to close
        # EXIT -> Close Position
        
        current_qty = self.current_positions.get(signal.symbol, 0)
        direction = signal.signal_type
        
        order_type = 'MKT' # Default to Market
        order_direction = None
        order_qty = 0

        if direction == 'LONG':
             order_direction = 'BUY'
             order_qty = quantity
        elif direction == 'SHORT':
             order_direction = 'SELL'
             order_qty = quantity
        elif direction == 'EXIT':
            if current_qty > 0:
                order_direction = 'SELL'
                order_qty = current_qty
            elif current_qty < 0:
                order_direction = 'BUY'
                order_qty = abs(current_qty)
        
        if order_direction and order_qty > 0:
            return OrderEvent(
                symbol=signal.symbol,
                order_type=order_type,
                quantity=order_qty,
                direction=order_direction
            )
        return None

    def _process_fill(self, fill: ExecutionEvent) -> None:
        """
        Internal: Core logic for position/cash update on fill.
        """
        symbol = fill.symbol
        qty = fill.filled_quantity
        price = fill.avg_fill_price
        
        # If simulated price is 0, try to fetch real price from data handler for realism
        if price <= 0.0:
            latest = self.bars.get_latest_bars(symbol, 1)
            if latest is not None and not latest.empty:
                price = latest['Close'].iloc[-1]
            else:
                 self.logger.warning(f"Fill price 0 and no data for {symbol}. Using 0.0 for calculations.")

        # Direction multiplier
        direction = 1 if fill.direction == 'BUY' else -1
        signed_qty = qty * direction
        
        cost = qty * price
        commission = fill.commission
        total_cost = cost + commission # Cash outflow (if buy) or inflow (if sell but minus cost?)
        # Actually:
        # Buy: Cash -= (Price * Qty + Comm)
        # Sell: Cash += (Price * Qty - Comm)
        
        cash_change = 0.0
        if fill.direction == 'BUY':
            cash_change = -1 * (cost + commission)
        else:
            cash_change = (cost - commission)

        self.cash += cash_change

        # Position Tracking & P&L
        current_qty = self.current_positions.get(symbol, 0)
        current_avg = self.avg_cost.get(symbol, 0.0)
        
        new_qty = current_qty + signed_qty
        
        # P&L Logic
        # If reducing position (closing), realize P&L
        # Reducing means: (prev > 0 and signed_qty < 0) OR (prev < 0 and signed_qty > 0)
        is_closing = (current_qty > 0 and signed_qty < 0) or (current_qty < 0 and signed_qty > 0)
        
        if is_closing:
            # We are closing 'qty' amount (or less if flipping)
            qty_closing = min(abs(current_qty), abs(signed_qty))
            
            # Realized P&L = (Exit Price - Entry Price) * Qty * Direction
            # For Long Exit: (Price - Avg) * Qty
            # For Short Exit: (Avg - Price) * Qty
            
            trade_pnl = 0.0
            if current_qty > 0: # Long close
                trade_pnl = (price - current_avg) * qty_closing
            else: # Short close
                trade_pnl = (current_avg - price) * qty_closing
            
            self.realized_pnl += trade_pnl
            self.realized_pnl -= commission # Commission is realized loss immediately
            
            # Log Trade
            self._log("TRADE_CLOSE", {
                "symbol": symbol, 
                "pnl": trade_pnl, 
                "qty": qty_closing, 
                "entry": current_avg, 
                "exit": price
            })

            # Check for position flip (e.g. Long 10, Sell 20 -> Short 10)
            remaining_signed = current_qty + signed_qty
            if (current_qty > 0 and remaining_signed < 0) or (current_qty < 0 and remaining_signed > 0):
                 # We flipped. The "new" position starts at this fill price.
                 self.avg_cost[symbol] = price
        
        elif current_qty == 0:
             # Opening new
             self.avg_cost[symbol] = price
             self.realized_pnl -= commission
        else:
             # Increasing position (Averaging)
             # New Avg = (OldVal + NewVal) / NewQty
             old_val = abs(current_qty) * current_avg
             new_val = abs(signed_qty) * price
             total_qty_abs = abs(current_qty) + abs(signed_qty)
             if total_qty_abs > 0:
                self.avg_cost[symbol] = (old_val + new_val) / total_qty_abs
             self.realized_pnl -= commission

        # Update Position
        if new_qty == 0:
            if symbol in self.current_positions:
                del self.current_positions[symbol]
            if symbol in self.avg_cost:
                del self.avg_cost[symbol]
        else:
            self.current_positions[symbol] = new_qty
            
        self._log("FILL_PROCESSED", {
            "symbol": symbol, 
            "filled": signed_qty, 
            "new_pos": self.current_positions.get(symbol, 0),
            "cash": self.cash
        })

    def _update_unrealized_pnl(self):
        """
        Internal: Calculate Unrealized P&L based on latest market data.
        """
        unrealized = 0.0
        for symbol, qty in self.current_positions.items():
            cost_basis = self.avg_cost.get(symbol, 0.0)
            
            # Get latest price
            current_price = cost_basis # Default to cost if no data (no P&L)
            latest = self.bars.get_latest_bars(symbol, 1)
            if latest is not None and not latest.empty:
                current_price = latest['Close'].iloc[-1]
            
            # P&L
            if qty > 0:
                unrealized += (current_price - cost_basis) * qty
            else:
                unrealized += (cost_basis - current_price) * abs(qty)
        
        self.unrealized_pnl = unrealized

    def _update_equity(self):
        """
        Internal: Update total equity.
        """
        self.total_equity = self.cash + self.unrealized_pnl + 0 # Positions value is implicitly in (Cash - Cost) + Unrealized?
        # WAIT. 
        # Total Equity = Cash + Market Value of Positions.
        # My "Cash" calculation above deducted Cost. 
        # So "Cash" is "Free Cash". 
        # Market Value of Pos = Cost Basis + Unrealized P&L.
        # So Equity = Free Cash + Cost Basis + Unrealized P&L. (Correct).
        
        # Let's double check standard accounting:
        # Equity = Cash Balance + sum(Qty * MarketPrice)
        # My Cash Balance was reduced by Cost.
        # So yes: Equity = Cash + sum(Qty * Mkt)
        
        mkt_value = 0.0
        for symbol, qty in self.current_positions.items():
             # Get latest price
            price = self.avg_cost.get(symbol, 0.0)
            latest = self.bars.get_latest_bars(symbol, 1)
            if latest is not None and not latest.empty:
                price = latest['Close'].iloc[-1]
            
            # For short positions?
            # Long: Equity += Qty * Price
            # Short: Equity -= |Qty| * Price (Liability)
            # Actually standard Equity = Cash + MV. 
            # If I sold short 10 @ 100. Cash += 1000. Pos = -10. 
            # If price stays 100. MV = -10 * 100 = -1000. 
            # Equity = 1000 + (-1000) = 0? (Assuming started 0). Correct. I owe stock.
            
            mkt_value += qty * price
            
        self.total_equity = self.cash + mkt_value

    def _emit_state(self):
        """
        Publish PortfolioStateEvent.
        """
        event = PortfolioStateEvent(
            positions=self.current_positions.copy(),
            total_equity=self.total_equity,
            cash=self.cash,
            unrealized_pnl=self.unrealized_pnl,
            realized_pnl=self.realized_pnl,
            timestamp=datetime.utcnow()
        )
        self.events.put(event)

    def create_equity_curve_dataframe(self) -> None:
        """
        Creates a pandas DataFrame from the equity curve.
        Basic implementation for reporting compatibility.
        """
        # In a real system we'd track this history properly.
        # For now, we'll just use the current state as a placeholder 
        # or we should store history in update_timeindex/market.
        pass

    def output_summary_stats(self) -> list:
        """
        Creates a list of summary statistics for the portfolio.
        """
        stats = [
            ("Total Equity", "$%0.2f" % self.total_equity),
            ("Initial Capital", "$%0.2f" % self.initial_capital),
            ("Realized P&L", "$%0.2f" % self.realized_pnl),
            ("Unrealized P&L", "$%0.2f" % self.unrealized_pnl),
            ("Cash", "$%0.2f" % self.cash),
        ]
        return stats

    def _log(self, event_type: str, data: dict):
        """
        Structured logging.
        """
        log_entry = {
            "timestamp": datetime.utcnow().isoformat(),
            "event": event_type,
            "data": data
        }
        self.logger.info(json.dumps(log_entry))
