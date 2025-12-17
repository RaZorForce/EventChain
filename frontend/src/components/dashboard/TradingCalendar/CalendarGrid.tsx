import { DayCell } from './DayCell';
import { WeeklySummary, type WeekSummary } from './WeeklySummary';
import type { CalendarDay } from '@/types/dashboard';

interface CalendarGridProps {
  year: number;
  month: number;
  calendarData: CalendarDay[];
}

const WEEKDAYS = ['Sun', 'Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat'];

export function CalendarGrid({ year, month, calendarData }: CalendarGridProps) {
  // Get first day of month and number of days
  const firstDay = new Date(year, month, 1);
  const lastDay = new Date(year, month + 1, 0);
  const daysInMonth = lastDay.getDate();
  const startDayOfWeek = firstDay.getDay();

  // Create a map for quick lookup
  const dataMap = new Map<string, CalendarDay>();
  calendarData.forEach((day) => {
    dataMap.set(day.date, day);
  });

  // Build calendar grid
  const weeks: (CalendarDay | null)[][] = [];
  let currentWeek: (CalendarDay | null)[] = [];

  // Fill in empty days at start
  for (let i = 0; i < startDayOfWeek; i++) {
    currentWeek.push(null);
  }

  // Fill in days of month
  for (let day = 1; day <= daysInMonth; day++) {
    const dateStr = `${year}-${String(month + 1).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
    const dayData = dataMap.get(dateStr) || null;
    currentWeek.push(dayData);

    if (currentWeek.length === 7) {
      weeks.push(currentWeek);
      currentWeek = [];
    }
  }

  // Fill in empty days at end
  if (currentWeek.length > 0) {
    while (currentWeek.length < 7) {
      currentWeek.push(null);
    }
    weeks.push(currentWeek);
  }

  // Calculate weekly summaries
  const weekSummaries: WeekSummary[] = weeks.map((week, index) => {
    const weekData = week.filter((d): d is CalendarDay => d !== null && d.hasData);
    return {
      weekNumber: index + 1,
      pnl: weekData.reduce((sum, d) => sum + d.pnl, 0),
      tradingDays: weekData.filter((d) => d.tradeCount > 0).length,
    };
  });

  return (
    <div className="flex gap-2">
      {/* Main calendar grid */}
      <div className="flex-1">
        {/* Weekday headers */}
        <div className="grid grid-cols-7 gap-px mb-1">
          {WEEKDAYS.map((day) => (
            <div key={day} className="text-xs text-muted-foreground text-center py-1">
              {day}
            </div>
          ))}
        </div>

        {/* Calendar days */}
        <div className="grid grid-cols-7 gap-px">
          {weeks.flat().map((day, index) => {
            const dayOfMonth = index - startDayOfWeek + 1;
            const isCurrentMonth = dayOfMonth >= 1 && dayOfMonth <= daysInMonth;

            return (
              <DayCell
                key={index}
                day={day}
                dayOfMonth={isCurrentMonth ? dayOfMonth : 0}
                isCurrentMonth={isCurrentMonth}
              />
            );
          })}
        </div>
      </div>

      {/* Weekly summary column */}
      <div className="w-16">
        <div className="text-xs text-muted-foreground text-center py-1 mb-1">&nbsp;</div>
        <WeeklySummary weeks={weekSummaries} />
      </div>
    </div>
  );
}
