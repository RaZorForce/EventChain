import { Card } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { DonutChart, TripleDonutChart } from '../Charts/DonutChart';
import { Info, Copy } from 'lucide-react';
import type { KPIMetrics } from '@/types/dashboard';

interface KPICardsProps {
  kpis: KPIMetrics;
}

function formatCurrency(value: number): string {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(value);
}

function formatPercent(value: number): string {
  return `${value.toFixed(2)}%`;
}

export function KPICards({ kpis }: KPICardsProps) {
  return (
    <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
      {/* Net P&L Card */}
      <Card className="p-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <span className="text-sm text-muted-foreground">Net P&L</span>
            <Info className="h-3.5 w-3.5 text-muted-foreground" />
            <Badge variant="secondary" className="text-xs px-1.5 py-0">
              {kpis.netPnL.tradeCount}
            </Badge>
          </div>
          <button className="text-muted-foreground hover:text-foreground">
            <Copy className="h-4 w-4" />
          </button>
        </div>
        <div className={`text-2xl font-bold mt-2 ${kpis.netPnL.value >= 0 ? 'text-profit' : 'text-loss'}`}>
          {formatCurrency(kpis.netPnL.value)}
        </div>
      </Card>

      {/* Trade Expectancy Card */}
      <Card className="p-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <span className="text-sm text-muted-foreground">Trade expectancy</span>
            <Info className="h-3.5 w-3.5 text-muted-foreground" />
          </div>
          <button className="text-muted-foreground hover:text-foreground">
            <Copy className="h-4 w-4" />
          </button>
        </div>
        <div className={`text-2xl font-bold mt-2 ${kpis.expectancy.value >= 0 ? 'text-profit' : 'text-loss'}`}>
          {formatCurrency(kpis.expectancy.value)}
        </div>
      </Card>

      {/* Profit Factor Card */}
      <Card className="p-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <span className="text-sm text-muted-foreground">Profit factor</span>
            <Info className="h-3.5 w-3.5 text-muted-foreground" />
          </div>
          <DonutChart
            value={kpis.profitFactor.value}
            maxValue={3}
            color={kpis.profitFactor.value >= 1 ? 'hsl(var(--profit))' : 'hsl(var(--loss))'}
          />
        </div>
        <div className="text-2xl font-bold mt-2">
          {kpis.profitFactor.value.toFixed(2)}
        </div>
      </Card>

      {/* Trade Win % Card */}
      <Card className="p-4">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-2">
            <span className="text-sm text-muted-foreground">Trade win %</span>
            <Info className="h-3.5 w-3.5 text-muted-foreground" />
          </div>
          <TripleDonutChart
            wins={kpis.winRate.wins}
            losses={kpis.winRate.losses}
            breakeven={kpis.winRate.breakeven}
          />
        </div>
        <div className="text-2xl font-bold mt-2">
          {formatPercent(kpis.winRate.percentage)}
        </div>
      </Card>

      {/* Avg Win/Loss Card */}
      <Card className="p-4">
        <div className="flex items-center gap-2">
          <span className="text-sm text-muted-foreground">Avg win/loss trade</span>
          <Info className="h-3.5 w-3.5 text-muted-foreground" />
        </div>
        <div className="flex items-center justify-between mt-2">
          <div className="text-2xl font-bold">
            {kpis.avgWinLoss.ratio.toFixed(2)}
          </div>
          <div className="text-right">
            <div className="text-sm text-profit">{formatCurrency(kpis.avgWinLoss.avgWin)}</div>
            <div className="text-sm text-loss">{formatCurrency(kpis.avgWinLoss.avgLoss)}</div>
          </div>
        </div>
      </Card>
    </div>
  );
}
