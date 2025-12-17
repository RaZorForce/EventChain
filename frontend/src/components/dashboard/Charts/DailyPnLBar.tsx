import { Card } from '@/components/ui/card';
import { Info } from 'lucide-react';
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from 'recharts';
import type { DailyPnLPoint } from '@/types/dashboard';

interface DailyPnLBarChartProps {
  data: DailyPnLPoint[];
}

function formatCurrency(value: number): string {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(value);
}

function formatDate(dateStr: string): string {
  const date = new Date(dateStr);
  return date.toLocaleDateString('en-US', { month: '2-digit', day: '2-digit', year: '2-digit' });
}

export function DailyPnLBarChart({ data }: DailyPnLBarChartProps) {
  return (
    <Card className="p-4 h-full">
      <div className="flex items-center gap-2 mb-4">
        <span className="text-sm font-medium">Net daily P&L</span>
        <Info className="h-3.5 w-3.5 text-muted-foreground" />
      </div>
      <div className="h-[200px]">
        <ResponsiveContainer width="100%" height="100%">
          <BarChart
            data={data}
            margin={{ top: 10, right: 10, left: 0, bottom: 0 }}
          >
            <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" vertical={false} />
            <XAxis
              dataKey="date"
              tickFormatter={formatDate}
              tick={{ fontSize: 11, fill: 'hsl(var(--muted-foreground))' }}
              tickLine={false}
              axisLine={false}
              interval="preserveStartEnd"
            />
            <YAxis
              tickFormatter={(v) => formatCurrency(v)}
              tick={{ fontSize: 11, fill: 'hsl(var(--muted-foreground))' }}
              tickLine={false}
              axisLine={false}
              width={80}
            />
            <Tooltip
              content={({ active, payload }) => {
                if (active && payload && payload.length) {
                  const point = payload[0].payload as DailyPnLPoint;
                  return (
                    <div className="bg-popover border border-border rounded-md shadow-md p-2">
                      <p className="text-xs text-muted-foreground">{formatDate(point.date)}</p>
                      <p className={`text-sm font-medium ${point.pnl >= 0 ? 'text-profit' : 'text-loss'}`}>
                        {formatCurrency(point.pnl)}
                      </p>
                      <p className="text-xs text-muted-foreground">{point.tradeCount} trades</p>
                    </div>
                  );
                }
                return null;
              }}
            />
            <Bar dataKey="pnl" radius={[2, 2, 0, 0]}>
              {data.map((entry, index) => (
                <Cell
                  key={`cell-${index}`}
                  fill={entry.pnl >= 0 ? 'hsl(var(--profit))' : 'hsl(var(--loss))'}
                />
              ))}
            </Bar>
          </BarChart>
        </ResponsiveContainer>
      </div>
    </Card>
  );
}
