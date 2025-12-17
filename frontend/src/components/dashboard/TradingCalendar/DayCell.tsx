import type { CalendarDay } from '@/types/dashboard';

interface DayCellProps {
  day: CalendarDay | null;
  dayOfMonth: number;
  isCurrentMonth: boolean;
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

export function DayCell({ day, dayOfMonth, isCurrentMonth }: DayCellProps) {
  if (!isCurrentMonth) {
    return <div className="h-20 bg-muted/20" />;
  }

  const hasData = day?.hasData && day.tradeCount > 0;
  const isProfit = day && day.pnl > 0;
  const isLoss = day && day.pnl < 0;

  let bgColor = 'bg-muted/10';
  if (isProfit) bgColor = 'bg-profit/20';
  if (isLoss) bgColor = 'bg-loss/20';

  return (
    <div className={`h-20 p-1 border border-border/50 ${bgColor} relative`}>
      <div className="text-xs text-muted-foreground">{dayOfMonth}</div>
      {hasData && day && (
        <div className="mt-1 space-y-0.5">
          <div className={`text-xs font-medium ${isProfit ? 'text-profit' : isLoss ? 'text-loss' : ''}`}>
            {formatCurrency(day.pnl)}
          </div>
          <div className="text-[10px] text-muted-foreground">
            {day.tradeCount} {day.tradeCount === 1 ? 'trade' : 'trades'}
          </div>
          {day.winRate > 0 && (
            <div className="text-[10px] text-muted-foreground">
              {day.winRate.toFixed(day.winRate % 1 === 0 ? 0 : 2)}%
            </div>
          )}
        </div>
      )}
    </div>
  );
}
