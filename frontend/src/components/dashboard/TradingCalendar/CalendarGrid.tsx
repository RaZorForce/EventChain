import { DayCell } from './DayCell';
import { WeeklySummary, type WeekSummary } from './WeeklySummary';
import type { CalendarDay } from '@/types/dashboard';

interface CalendarGridProps {
  year: number;
  month: number;
  calendarData: CalendarDay[];
  showWeekends?: boolean;
}

const WEEKDAYS_FULL = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'];
const WEEKDAYS_WORKDAYS = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri'];

export function CalendarGrid({ year, month, calendarData, showWeekends = true }: CalendarGridProps) {
  const weekdays = showWeekends ? WEEKDAYS_FULL : WEEKDAYS_WORKDAYS;
  const numCols = showWeekends ? 7 : 5;
  // Get first day of month and number of days
  const firstDay = new Date(year, month, 1);
  const lastDay = new Date(year, month + 1, 0);
  const daysInMonth = lastDay.getDate();
  // Convert Sunday (0) to 6, and shift Monday to be 0
  const startDayOfWeek = (firstDay.getDay() + 6) % 7;

  // Create a map for quick lookup
  const dataMap = new Map<string, CalendarDay>();
  calendarData.forEach((day) => {
    dataMap.set(day.date, day);
  });

  // Build calendar grid
  const weeks: (CalendarDay | null)[][] = [];
  let currentWeek: (CalendarDay | null)[] = [];

  // Adjust start day for weekend filtering
  let adjustedStartDayOfWeek = startDayOfWeek;
  if (!showWeekends) {
    // If hiding weekends, adjust for Saturday (5) and Sunday (6)
    if (startDayOfWeek >= 5) {
      adjustedStartDayOfWeek = 0; // Start on Monday if month starts on weekend
    }
  }

  // Fill in empty days at start
  for (let i = 0; i < adjustedStartDayOfWeek; i++) {
    currentWeek.push(null);
  }

  // Fill in days of month
  for (let day = 1; day <= daysInMonth; day++) {
    const currentDay = new Date(year, month, day);
    const dayOfWeek = (currentDay.getDay() + 6) % 7; // Convert to Monday=0 format
    
    // Skip weekends if showWeekends is false
    if (!showWeekends && (dayOfWeek === 5 || dayOfWeek === 6)) {
      continue;
    }

    const dateStr = `${year}-${String(month + 1).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
    const dayData = dataMap.get(dateStr) || null;
    currentWeek.push(dayData);

    if (currentWeek.length === numCols) {
      weeks.push(currentWeek);
      currentWeek = [];
    }
  }

  // Fill in empty days at end
  if (currentWeek.length > 0) {
    while (currentWeek.length < numCols) {
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
        <div className={`grid gap-px mb-1 ${showWeekends ? 'grid-cols-7' : 'grid-cols-5'}`}>
          {weekdays.map((day) => (
            <div key={day} className="text-xs text-muted-foreground text-center py-1 flex items-center justify-center">
              {day}
            </div>
          ))}
        </div>

        {/* Calendar days */}
        <div className={`grid gap-px ${showWeekends ? 'grid-cols-7' : 'grid-cols-5'}`}>
          {weeks.flat().map((day, index) => {
            // Calculate day of month based on actual calendar data if available
            const dayOfMonth = day ? new Date(day.date).getDate() : 0;
            const isCurrentMonth = day !== null;

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
        <div className="text-xs text-muted-foreground text-center py-1 mb-1">Weekly P&L</div>
        <WeeklySummary weeks={weekSummaries} />
      </div>
    </div>
  );
}
