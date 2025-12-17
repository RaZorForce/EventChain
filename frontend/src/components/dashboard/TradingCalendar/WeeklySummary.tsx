interface WeekSummary {
  weekNumber: number;
  pnl: number;
  tradingDays: number;
}

interface WeeklySummaryProps {
  weeks: WeekSummary[];
}

function formatCurrency(value: number): string {
  if (value === 0) return '$0';
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value);
}

export function WeeklySummary({ weeks }: WeeklySummaryProps) {
  return (
    <div className="space-y-2">
      {weeks.map((week) => (
        <div
          key={week.weekNumber}
          className="h-20 p-2 border border-border/50 bg-muted/10 flex flex-col justify-center"
        >
          <div className={`text-xs font-medium ${week.pnl > 0 ? 'text-profit' : week.pnl < 0 ? 'text-loss' : ''}`}>
            {formatCurrency(week.pnl)}
          </div>
          <div className="text-[10px] text-muted-foreground">
            {week.tradingDays} {week.tradingDays === 1 ? 'day' : 'days'}
          </div>
        </div>
      ))}
    </div>
  );
}

export type { WeekSummary };
