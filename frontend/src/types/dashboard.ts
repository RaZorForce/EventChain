export interface DashboardData {
  lastImport: string;
  kpis: KPIMetrics;
  zellaScore: ZellaScore;
  recentTrades: Trade[];
  openPositions: Position[];
  cumulativePnL: CumulativePnLPoint[];
  dailyPnL: DailyPnLPoint[];
  calendarData: CalendarDay[];
  tradeTimeDistribution: TradeTimePoint[];
}

export interface KPIMetrics {
  netPnL: {
    value: number;
    tradeCount: number;
    percentChange?: number;
  };
  expectancy: {
    value: number;
    percentChange?: number;
  };
  profitFactor: {
    value: number;
    grossProfit: number;
    grossLoss: number;
  };
  winRate: {
    percentage: number;
    wins: number;
    losses: number;
    breakeven: number;
  };
  avgWinLoss: {
    avgWin: number;
    avgLoss: number;
    ratio: number;
  };
}

export interface ZellaScoreMetrics {
  winRate: number;
  profitFactor: number;
  avgWinLoss: number;
  recoveryFactor: number;
  maxDrawdown: number;
  consistency: number;
}

export interface ZellaScore {
  overall: number;
  metrics: ZellaScoreMetrics;
}

export interface Trade {
  id: string;
  date: string;
  closeDate: string;
  symbol: string;
  side: 'LONG' | 'SHORT';
  entryPrice: number;
  exitPrice: number;
  quantity: number;
  netPnL: number;
  commission: number;
  status: 'WIN' | 'LOSS' | 'BREAKEVEN';
}

export interface Position {
  id: string;
  symbol: string;
  side: 'LONG' | 'SHORT';
  quantity: number;
  avgEntryPrice: number;
  currentPrice: number;
  unrealizedPnL: number;
  marketValue: number;
}

export interface CumulativePnLPoint {
  date: string;
  value: number;
}

export interface DailyPnLPoint {
  date: string;
  pnl: number;
  tradeCount: number;
}

export interface CalendarDay {
  date: string;
  pnl: number;
  tradeCount: number;
  winRate: number;
  hasData: boolean;
}

export interface TradeTimePoint {
  time: string;
  hour: number;
  pnl: number;
  tradeId?: string;
}

export interface DateRange {
  from: Date;
  to: Date;
}

export interface Account {
  id: string;
  name: string;
  broker: string;
}
