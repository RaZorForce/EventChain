import { Card } from '@/components/ui/card';
import { Eye, Maximize2 } from 'lucide-react';
import {
  ScatterChart,
  Scatter,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell,
} from 'recharts';
import type { TradeTimePoint } from '@/types/dashboard';

interface TradeTimePerformanceProps {
  data: TradeTimePoint[];
}

function formatCurrency(value: number): string {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value);
}

export function TradeTimePerformance({ data }: TradeTimePerformanceProps) {
  // Transform data for scatter plot - use hour as numeric x value
  const scatterData = data.map((point) => ({
    ...point,
    x: point.hour + (parseInt(point.time.split(':')[1]) / 60), // Convert time to decimal hour
  }));

  return (
    <Card className="p-4">
      <div className="flex items-center justify-between mb-4">
        <span className="text-sm font-medium">Trade time</span>
        <div className="flex items-center gap-2">
          <button className="text-muted-foreground hover:text-foreground">
            <Eye className="h-4 w-4" />
          </button>
          <button className="text-muted-foreground hover:text-foreground">
            <Maximize2 className="h-4 w-4" />
          </button>
        </div>
      </div>
      <div className="h-[200px]">
        <ResponsiveContainer width="100%" height="100%">
          <ScatterChart margin={{ top: 10, right: 10, left: 0, bottom: 0 }}>
            <CartesianGrid strokeDasharray="3 3" stroke="hsl(var(--border))" />
            <XAxis
              type="number"
              dataKey="x"
              domain={[5, 23]}
              ticks={[5, 7, 9, 11, 13, 15, 17, 19, 21, 23]}
              tickFormatter={(v) => `${v}:00`}
              tick={{ fontSize: 10, fill: 'hsl(var(--muted-foreground))' }}
              tickLine={false}
              axisLine={false}
            />
            <YAxis
              type="number"
              dataKey="pnl"
              tickFormatter={(v) => formatCurrency(v)}
              tick={{ fontSize: 10, fill: 'hsl(var(--muted-foreground))' }}
              tickLine={false}
              axisLine={false}
              width={60}
            />
            <Tooltip
              content={({ active, payload }) => {
                if (active && payload && payload.length) {
                  const point = payload[0].payload as TradeTimePoint & { x: number };
                  return (
                    <div className="bg-popover border border-border rounded-md shadow-md p-2">
                      <p className="text-xs text-muted-foreground">{point.time}</p>
                      <p className={`text-sm font-medium ${point.pnl >= 0 ? 'text-profit' : 'text-loss'}`}>
                        {formatCurrency(point.pnl)}
                      </p>
                    </div>
                  );
                }
                return null;
              }}
            />
            <Scatter data={scatterData} fill="hsl(270, 70%, 60%)">
              {scatterData.map((entry, index) => (
                <Cell
                  key={`cell-${index}`}
                  fill={entry.pnl >= 0 ? 'hsl(var(--profit))' : 'hsl(var(--loss))'}
                />
              ))}
            </Scatter>
          </ScatterChart>
        </ResponsiveContainer>
      </div>
    </Card>
  );
}
