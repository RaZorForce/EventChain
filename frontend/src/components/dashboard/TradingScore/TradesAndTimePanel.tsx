import { Card } from '@/components/ui/card';
import { RecentTradesTable } from './RecentTradesTable';
import { TradeTimePerformance } from './TradeTimePerformance';
import type { Trade, Position, TradeTimePoint } from '@/types/dashboard';

interface TradesAndTimeProps {
  recentTrades: Trade[];
  openPositions: Position[];
  tradeTimeData: TradeTimePoint[];
}

export function TradesAndTimePanel({
  recentTrades,
  openPositions,
  tradeTimeData,
}: TradesAndTimeProps) {
  return (
    <div className="space-y-4">
      {/* Recent Trades / Open Positions Tabs */}
      <Card className="p-4">
        <RecentTradesTable trades={recentTrades} positions={openPositions} />
      </Card>

      {/* Trade Time Performance Scatter Plot */}
      <TradeTimePerformance data={tradeTimeData} />
    </div>
  );
}