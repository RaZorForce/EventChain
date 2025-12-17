import { Card } from '@/components/ui/card';
import { Info } from 'lucide-react';
import { RadarChart } from './RadarChart';
import { ScoreProgress } from './ScoreProgress';
import type { ZellaScore } from '@/types/dashboard';

interface ZellaScoreCardProps {
  zellaScore: ZellaScore;
}

export function ZellaScoreCard({ zellaScore }: ZellaScoreCardProps) {
  return (
    <Card className="p-4">
      <div className="flex items-center gap-2 mb-4">
        <span className="text-sm font-medium">Zella score</span>
        <Info className="h-3.5 w-3.5 text-muted-foreground" />
      </div>
      <RadarChart metrics={zellaScore.metrics} />
      <div className="mt-4">
        <ScoreProgress score={zellaScore.overall} />
      </div>
    </Card>
  );
}