import { PieChart, Pie, Cell, ResponsiveContainer } from 'recharts';

interface DonutChartProps {
  value: number;
  maxValue?: number;
  color?: string;
  size?: number;
  strokeWidth?: number;
}

export function DonutChart({
  value,
  maxValue = 100,
  color = 'hsl(var(--profit))',
  size = 60,
  strokeWidth = 8
}: DonutChartProps) {
  const percentage = Math.min((value / maxValue) * 100, 100);
  const data = [
    { name: 'filled', value: percentage },
    { name: 'empty', value: 100 - percentage },
  ];

  return (
    <div style={{ width: size, height: size }}>
      <ResponsiveContainer width="100%" height="100%">
        <PieChart>
          <Pie
            data={data}
            cx="50%"
            cy="50%"
            innerRadius={size / 2 - strokeWidth}
            outerRadius={size / 2}
            startAngle={90}
            endAngle={-270}
            dataKey="value"
            stroke="none"
          >
            <Cell fill={color} />
            <Cell fill="hsl(var(--muted))" />
          </Pie>
        </PieChart>
      </ResponsiveContainer>
    </div>
  );
}

interface TripleDonutChartProps {
  wins: number;
  losses: number;
  breakeven: number;
  size?: number;
}

export function TripleDonutChart({
  wins,
  losses,
  breakeven,
  size = 80
}: TripleDonutChartProps) {
  const data = [
    { name: 'wins', value: wins, color: 'hsl(var(--profit))' },
    { name: 'losses', value: losses, color: 'hsl(var(--loss))' },
    { name: 'breakeven', value: breakeven, color: 'hsl(var(--muted-foreground))' },
  ].filter(d => d.value > 0);

  return (
    <div style={{ width: size, height: size }} className="relative">
      <ResponsiveContainer width="100%" height="100%">
        <PieChart>
          <Pie
            data={data}
            cx="50%"
            cy="50%"
            innerRadius={size / 2 - 10}
            outerRadius={size / 2}
            dataKey="value"
            stroke="none"
          >
            {data.map((entry, index) => (
              <Cell key={`cell-${index}`} fill={entry.color} />
            ))}
          </Pie>
        </PieChart>
      </ResponsiveContainer>
      <div className="absolute inset-0 flex items-center justify-center">
        <div className="text-center">
          <div className="flex items-center gap-1 text-xs">
            <span className="text-profit font-semibold">{wins}</span>
            <span className="text-muted-foreground">/</span>
            <span className="text-loss font-semibold">{losses}</span>
            {breakeven > 0 && (
              <>
                <span className="text-muted-foreground">/</span>
                <span className="text-muted-foreground font-semibold">{breakeven}</span>
              </>
            )}
          </div>
        </div>
      </div>
    </div>
  );
}
