"""
Risk management module.

Monitors portfolio risk and emits PortfolioStateEvent when risk status changes.
"""
from src.engine.events import PortfolioStateEvent


class RiskManager:
    """
    Monitors portfolio risk metrics and enforces risk limits.
    """
    
    def __init__(self, max_drawdown: float = 0.20, max_position_size: float = 0.10):
        """
        Initialize risk manager.
        
        Args:
            max_drawdown: Maximum allowed drawdown (default 20%)
            max_position_size: Maximum position size as % of equity (default 10%)
        """
        self.max_drawdown = max_drawdown
        self.max_position_size = max_position_size
        self.risk_status = 'NORMAL'
        self.current_drawdown = 0.0
    
    def check_risk(self, total_equity: float, peak_equity: float, 
                   positions: dict, cash: float) -> tuple:
        """
        Check current risk status.
        
        Args:
            total_equity: Current total equity
            peak_equity: Peak equity achieved
            positions: Current positions {symbol: quantity}
            cash: Available cash
        
        Returns:
            (risk_status, risk_message) tuple
        """
        # Calculate drawdown
        if peak_equity > 0:
            self.current_drawdown = (peak_equity - total_equity) / peak_equity
        
        # Check drawdown limit
        if self.current_drawdown >= self.max_drawdown:
            self.risk_status = 'HALTED'
            return ('HALTED', f'Max drawdown exceeded: {self.current_drawdown:.2%}')
        elif self.current_drawdown >= self.max_drawdown * 0.8:
            self.risk_status = 'CRITICAL'
            return ('CRITICAL', f'Approaching max drawdown: {self.current_drawdown:.2%}')
        elif self.current_drawdown >= self.max_drawdown * 0.5:
            self.risk_status = 'WARNING'
            return ('WARNING', f'Elevated drawdown: {self.current_drawdown:.2%}')
        else:
            self.risk_status = 'NORMAL'
            return ('NORMAL', 'Risk within limits')
    
    def create_portfolio_state_event(self, positions: dict, total_equity: float,
                                     cash: float, unrealized_pnl: float, 
                                     realized_pnl: float) -> PortfolioStateEvent:
        """
        Create a PortfolioStateEvent with current risk status.
        
        Returns:
            PortfolioStateEvent with current portfolio and risk state
        """
        risk_status, risk_message = self.risk_status, ''
        
        return PortfolioStateEvent(
            positions=positions,
            total_equity=total_equity,
            cash=cash,
            unrealized_pnl=unrealized_pnl,
            realized_pnl=realized_pnl,
            max_drawdown=self.current_drawdown,
            risk_status=risk_status,
            risk_message=risk_message
        )
