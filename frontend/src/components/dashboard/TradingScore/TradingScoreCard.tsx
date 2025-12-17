import { Card } from '@/components/ui/card';
import { Info } from 'lucide-react';
import { RadarChart } from './RadarChart';
import { ScoreProgress } from './ScoreProgress';
import type { TradingScore } from '@/types/dashboard';

interface TradingScoreCardProps {
  tradingScore: TradingScore;
}

export function TradingScoreCard({ tradingScore }: TradingScoreCardProps) {
  return (
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
  );
}
