import { Card } from '@/components/ui/card';
import { Info } from 'lucide-react';
import { RadarChart } from './RadarChart';
import { ScoreProgress } from './ScoreProgress';
import { RecentTradesTable } from './RecentTradesTable';
import { TradeTimePerformance } from './TradeTimePerformance';
import type { TradingScore, Trade, Position, TradeTimePoint } from '@/types/dashboard';

interface TradingScorePanelProps {
  tradingScore: TradingScore;
  recentTrades: Trade[];
  openPositions: Position[];
  tradeTimeData: TradeTimePoint[];
}

export function TradingScorePanel({
  tradingScore,
  recentTrades,
  openPositions,
  tradeTimeData,
}: TradingScorePanelProps) {
  return (
    <div className="space-y-4">
      {/* Trading Score Card */}
      <Card className="p-4">
        <div className="flex items-center gap-2 mb-4">
          <span className="text-sm font-medium">Trading Score</span>
          <Info className="h-3.5 w-3.5 text-muted-foreground" />
        </div>
        <RadarChart metrics={tradingScore.metrics} />
        <div className="mt-4">
          <ScoreProgress score={tradingScore.overall} />
        </div>
      </Card>

      {/* Recent Trades / Open Positions Tabs */}
      <Card className="p-4">
        <RecentTradesTable trades={recentTrades} positions={openPositions} />
      </Card>

      {/* Trade Time Performance Scatter Plot */}
      <TradeTimePerformance data={tradeTimeData} />
    </div>
  );
}

export { RadarChart } from './RadarChart';
export { ScoreProgress } from './ScoreProgress';
export { RecentTradesTable } from './RecentTradesTable';
export { TradeTimePerformance } from './TradeTimePerformance';
export { TradingScoreCard } from './TradingScoreCard';
export { TradesAndTimePanel } from './TradesAndTimePanel';
