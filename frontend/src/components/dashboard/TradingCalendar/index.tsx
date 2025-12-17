import { useState } from 'react';
import { Card } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { ChevronLeft, ChevronRight, Settings, Maximize2 } from 'lucide-react';
import { CalendarGrid } from './CalendarGrid';
import type { CalendarDay } from '@/types/dashboard';

interface TradingCalendarProps {
  calendarData: CalendarDay[];
  initialDate?: Date;
}

function formatCurrency(value: number): string {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 0,
    maximumFractionDigits: 0,
  }).format(value);
}

export function TradingCalendar({ calendarData, initialDate }: TradingCalendarProps) {
  const [currentDate, setCurrentDate] = useState(initialDate || new Date());
  const year = currentDate.getFullYear();
  const month = currentDate.getMonth();

  const monthName = currentDate.toLocaleDateString('en-US', { month: 'long', year: 'numeric' });

  // Calculate monthly stats
  const monthData = calendarData.filter((day) => {
    const d = new Date(day.date);
    return d.getFullYear() === year && d.getMonth() === month;
  });
  const totalPnL = monthData.reduce((sum, d) => sum + d.pnl, 0);
  const tradingDays = monthData.filter((d) => d.tradeCount > 0).length;

  const goToPrevMonth = () => {
    setCurrentDate(new Date(year, month - 1, 1));
  };

  const goToNextMonth = () => {
    setCurrentDate(new Date(year, month + 1, 1));
  };

  const goToThisMonth = () => {
    setCurrentDate(new Date());
  };

  return (
    <Card className="p-4">
      {/* Calendar Header */}
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-2">
          <button
            onClick={goToPrevMonth}
            className="p-1 hover:bg-muted rounded"
          >
            <ChevronLeft className="h-4 w-4" />
          </button>
          <button
            onClick={goToNextMonth}
            className="p-1 hover:bg-muted rounded"
          >
            <ChevronRight className="h-4 w-4" />
          </button>
          <span className="text-sm font-medium">{monthName}</span>
          <button
            onClick={goToThisMonth}
            className="text-xs text-muted-foreground hover:text-foreground px-2 py-1 border rounded"
          >
            This month
          </button>
        </div>

        <div className="flex items-center gap-3">
          <Badge variant={totalPnL >= 0 ? 'success' : 'destructive'} className="text-xs">
            {formatCurrency(totalPnL)}
          </Badge>
          <span className="text-xs text-muted-foreground">{tradingDays} days</span>
          <button className="text-muted-foreground hover:text-foreground">
            <Settings className="h-4 w-4" />
          </button>
          <button className="text-muted-foreground hover:text-foreground">
            <Maximize2 className="h-4 w-4" />
          </button>
        </div>
      </div>

      {/* Calendar Grid */}
      <CalendarGrid year={year} month={month} calendarData={calendarData} />
    </Card>
  );
}

export { CalendarGrid } from './CalendarGrid';
export { DayCell } from './DayCell';
export { WeeklySummary } from './WeeklySummary';
