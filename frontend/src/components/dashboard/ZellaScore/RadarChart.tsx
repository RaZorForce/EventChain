import {
  Radar,
  RadarChart as RechartsRadarChart,
  PolarGrid,
  PolarAngleAxis,
  PolarRadiusAxis,
  ResponsiveContainer,
} from 'recharts';
import type { ZellaScoreMetrics } from '@/types/dashboard';

interface RadarChartProps {
  metrics: ZellaScoreMetrics;
}

export function RadarChart({ metrics }: RadarChartProps) {
  const data = [
    { subject: 'Win %', value: metrics.winRate, fullMark: 100 },
    { subject: 'Profit factor', value: metrics.profitFactor, fullMark: 100 },
    { subject: 'Avg win/loss', value: metrics.avgWinLoss, fullMark: 100 },
    { subject: 'Recovery factor', value: metrics.recoveryFactor, fullMark: 100 },
    { subject: 'Max drawdown', value: metrics.maxDrawdown, fullMark: 100 },
    { subject: 'Consistency', value: metrics.consistency, fullMark: 100 },
  ];

  return (
    <div className="h-[200px] w-full">
      <ResponsiveContainer width="100%" height="100%">
        <RechartsRadarChart cx="50%" cy="50%" outerRadius="70%" data={data}>
          <PolarGrid stroke="hsl(var(--border))" />
          <PolarAngleAxis
            dataKey="subject"
            tick={{ fontSize: 10, fill: 'hsl(var(--muted-foreground))' }}
          />
          <PolarRadiusAxis
            angle={30}
            domain={[0, 100]}
            tick={{ fontSize: 8, fill: 'hsl(var(--muted-foreground))' }}
            tickCount={5}
          />
          <Radar
            name="Score"
            dataKey="value"
            stroke="hsl(270, 70%, 60%)"
            fill="hsl(270, 70%, 60%)"
            fillOpacity={0.4}
          />
        </RechartsRadarChart>
      </ResponsiveContainer>
    </div>
  );
}
