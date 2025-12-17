import { Card } from '@/components/ui/card';
import { Info } from 'lucide-react';
import {
  AreaChart,
  Area,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
} from 'recharts';
import type { CumulativePnLPoint } from '@/types/dashboard';

interface CumulativePnLChartProps {
  data: CumulativePnLPoint[];
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

export function CumulativePnLChart({ data }: CumulativePnLChartProps) {
  // Determine if the final value is positive or negative for gradient color
  const lastValue = data.length > 0 ? data[data.length - 1].value : 0;
  const isPositive = lastValue >= 0;
  const gradientId = 'cumulativePnLGradient';

  return (
    <Card className="p-4 h-full">
      <div className="flex items-center gap-2 mb-4">
        <span className="text-sm font-medium">Daily net cumulative P&L</span>
        <Info className="h-3.5 w-3.5 text-muted-foreground" />
      </div>
      <div className="h-[300px]">
        <ResponsiveContainer width="100%" height="100%">
          <AreaChart
            data={data}
            margin={{ top: 10, right: 10, left: 0, bottom: 0 }}
          >
            <defs>
              <linearGradient id={gradientId} x1="0" y1="0" x2="0" y2="1">
                <stop
                  offset="5%"
                  stopColor={isPositive ? 'hsl(var(--profit))' : 'hsl(var(--loss))'}
                  stopOpacity={0.8}
                />
                <stop
                  offset="95%"
                  stopColor={isPositive ? 'hsl(var(--profit))' : 'hsl(var(--loss))'}
                  stopOpacity={0.1}
                />
              </linearGradient>
            </defs>
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
                  const point = payload[0].payload as CumulativePnLPoint;
                  return (
                    <div className="bg-popover border border-border rounded-md shadow-md p-2">
                      <p className="text-xs text-muted-foreground">{formatDate(point.date)}</p>
                      <p className={`text-sm font-medium ${point.value >= 0 ? 'text-profit' : 'text-loss'}`}>
                        {formatCurrency(point.value)}
                      </p>
                    </div>
                  );
                }
                return null;
              }}
            />
            <Area
              type="monotone"
              dataKey="value"
              stroke={isPositive ? 'hsl(var(--profit))' : 'hsl(var(--loss))'}
              strokeWidth={2}
              fill={`url(#${gradientId})`}
            />
          </AreaChart>
        </ResponsiveContainer>
      </div>
    </Card>
  );
}
