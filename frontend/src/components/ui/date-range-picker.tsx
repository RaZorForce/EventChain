"use client"

import * as React from "react"
import { format, subDays, subMonths, startOfMonth, endOfMonth, startOfQuarter, startOfYear } from "date-fns"
import { ChevronLeft, ChevronRight } from "lucide-react"
import { cn } from "@/lib/utils"
import { Button } from "@/components/ui/button"

interface DateRangePickerProps {
  startDate: Date | undefined
  endDate: Date | undefined
  onStartDateChange: (date: Date | undefined) => void
  onEndDateChange: (date: Date | undefined) => void
  className?: string
}

const presets = [
  { label: "Today", getValue: () => ({ start: new Date(), end: new Date() }) },
  { label: "This Week", getValue: () => ({ start: subDays(new Date(), 7), end: new Date() }) },
  { label: "This Month", getValue: () => ({ start: startOfMonth(new Date()), end: new Date() }) },
  { label: "Last 30 Days", getValue: () => ({ start: subDays(new Date(), 30), end: new Date() }) },
  { label: "Last Month", getValue: () => {
    const lastMonth = subMonths(new Date(), 1)
    return { start: startOfMonth(lastMonth), end: endOfMonth(lastMonth) }
  }},
  { label: "This Quarter", getValue: () => ({ start: startOfQuarter(new Date()), end: new Date() }) },
  { label: "YTD (year-to-date)", getValue: () => ({ start: startOfYear(new Date()), end: new Date() }) },
]

const DAYS = ["Su", "Mo", "Tu", "We", "Th", "Fr", "Sa"]
const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]

function getMonthData(year: number, month: number) {
  const firstDay = new Date(year, month, 1)
  const lastDay = new Date(year, month + 1, 0)
  const daysInMonth = lastDay.getDate()
  const startingDay = firstDay.getDay()

  const days: (number | null)[] = []

  // Add empty slots for days before the first of the month
  for (let i = 0; i < startingDay; i++) {
    days.push(null)
  }

  // Add all days of the month
  for (let i = 1; i <= daysInMonth; i++) {
    days.push(i)
  }

  return days
}

interface MonthCalendarProps {
  year: number
  month: number
  selectedDate: Date | undefined
  rangeStart: Date | undefined
  rangeEnd: Date | undefined
  onDateSelect: (date: Date) => void
  onMonthChange: (delta: number) => void
  onYearChange: (year: number) => void
}

function MonthCalendar({
  year,
  month,
  selectedDate,
  rangeStart,
  rangeEnd,
  onDateSelect,
  onMonthChange,
  onYearChange,
}: MonthCalendarProps) {
  const days = getMonthData(year, month)

  const isSelected = (day: number) => {
    if (!selectedDate) return false
    return selectedDate.getFullYear() === year &&
           selectedDate.getMonth() === month &&
           selectedDate.getDate() === day
  }

  const isInRange = (day: number) => {
    if (!rangeStart || !rangeEnd) return false
    const date = new Date(year, month, day)
    return date >= rangeStart && date <= rangeEnd
  }

  const isRangeStart = (day: number) => {
    if (!rangeStart) return false
    return rangeStart.getFullYear() === year &&
           rangeStart.getMonth() === month &&
           rangeStart.getDate() === day
  }

  const isRangeEnd = (day: number) => {
    if (!rangeEnd) return false
    return rangeEnd.getFullYear() === year &&
           rangeEnd.getMonth() === month &&
           rangeEnd.getDate() === day
  }

  return (
    <div className="p-2">
      {/* Month/Year Header */}
      <div className="flex items-center justify-between mb-2">
        <Button variant="ghost" size="icon" className="h-7 w-7" onClick={() => onMonthChange(-1)}>
          <ChevronLeft className="h-4 w-4" />
        </Button>
        <div className="flex items-center gap-1">
          <select
            value={month}
            onChange={(e) => onMonthChange(parseInt(e.target.value) - month)}
            className="bg-transparent text-sm font-medium cursor-pointer border-none focus:outline-none"
          >
            {MONTHS.map((m, i) => (
              <option key={m} value={i}>{m}</option>
            ))}
          </select>
          <select
            value={year}
            onChange={(e) => onYearChange(parseInt(e.target.value))}
            className="bg-transparent text-sm font-medium cursor-pointer border-none focus:outline-none"
          >
            {Array.from({ length: 10 }, (_, i) => year - 5 + i).map((y) => (
              <option key={y} value={y}>{y}</option>
            ))}
          </select>
        </div>
        <Button variant="ghost" size="icon" className="h-7 w-7" onClick={() => onMonthChange(1)}>
          <ChevronRight className="h-4 w-4" />
        </Button>
      </div>

      {/* Days Header */}
      <div className="grid grid-cols-7 gap-0 mb-1">
        {DAYS.map((day) => (
          <div key={day} className="h-8 flex items-center justify-center text-xs text-muted-foreground font-medium">
            {day}
          </div>
        ))}
      </div>

      {/* Days Grid */}
      <div className="grid grid-cols-7 gap-0">
        {days.map((day, index) => (
          <div key={index} className="h-8 flex items-center justify-center">
            {day !== null ? (
              <button
                onClick={() => onDateSelect(new Date(year, month, day))}
                className={cn(
                  "h-8 w-8 rounded-full text-sm transition-colors",
                  "hover:bg-accent hover:text-accent-foreground",
                  isSelected(day) && "bg-primary text-primary-foreground",
                  isInRange(day) && !isRangeStart(day) && !isRangeEnd(day) && "bg-accent/50",
                  (isRangeStart(day) || isRangeEnd(day)) && "bg-primary text-primary-foreground"
                )}
              >
                {day}
              </button>
            ) : null}
          </div>
        ))}
      </div>
    </div>
  )
}

export function DateRangePicker({
  startDate,
  endDate,
  onStartDateChange,
  onEndDateChange,
  className,
}: DateRangePickerProps) {
  const [leftMonth, setLeftMonth] = React.useState(() => {
    const date = startDate || new Date()
    return { year: date.getFullYear(), month: date.getMonth() }
  })

  const [rightMonth, setRightMonth] = React.useState(() => {
    const date = endDate || new Date()
    const nextMonth = new Date(date.getFullYear(), date.getMonth() + 1, 1)
    return { year: nextMonth.getFullYear(), month: nextMonth.getMonth() }
  })

  const handleLeftMonthChange = (delta: number) => {
    setLeftMonth((prev) => {
      const newDate = new Date(prev.year, prev.month + delta, 1)
      return { year: newDate.getFullYear(), month: newDate.getMonth() }
    })
  }

  const handleRightMonthChange = (delta: number) => {
    setRightMonth((prev) => {
      const newDate = new Date(prev.year, prev.month + delta, 1)
      return { year: newDate.getFullYear(), month: newDate.getMonth() }
    })
  }

  const handlePresetClick = (preset: typeof presets[number]) => {
    const { start, end } = preset.getValue()
    onStartDateChange(start)
    onEndDateChange(end)
    setLeftMonth({ year: start.getFullYear(), month: start.getMonth() })
    setRightMonth({ year: end.getFullYear(), month: end.getMonth() })
  }

  return (
    <div className={cn("flex bg-background border rounded-lg shadow-lg", className)}>
      {/* Calendars */}
      <div className="flex border-r">
        {/* Left Calendar */}
        <div className="border-r">
          <MonthCalendar
            year={leftMonth.year}
            month={leftMonth.month}
            selectedDate={startDate}
            rangeStart={startDate}
            rangeEnd={endDate}
            onDateSelect={onStartDateChange}
            onMonthChange={handleLeftMonthChange}
            onYearChange={(year) => setLeftMonth((prev) => ({ ...prev, year }))}
          />
        </div>

        {/* Arrow between calendars */}
        <div className="flex items-start justify-center pt-4 px-2">
          <span className="text-muted-foreground">→</span>
        </div>

        {/* Right Calendar */}
        <div>
          <div className="p-2 text-center text-sm font-medium text-muted-foreground">
            End Date
          </div>
          <MonthCalendar
            year={rightMonth.year}
            month={rightMonth.month}
            selectedDate={endDate}
            rangeStart={startDate}
            rangeEnd={endDate}
            onDateSelect={onEndDateChange}
            onMonthChange={handleRightMonthChange}
            onYearChange={(year) => setRightMonth((prev) => ({ ...prev, year }))}
          />
        </div>
      </div>

      {/* Presets */}
      <div className="flex flex-col p-2 min-w-[140px]">
        {presets.map((preset) => (
          <button
            key={preset.label}
            onClick={() => handlePresetClick(preset)}
            className="text-left px-3 py-1.5 text-sm hover:bg-accent rounded transition-colors"
          >
            {preset.label}
          </button>
        ))}
      </div>
    </div>
  )
}

// Modal version for use in forms
interface DateRangePickerInputProps {
  startDate: string
  endDate: string
  onStartDateChange: (date: string) => void
  onEndDateChange: (date: string) => void
}

export function DateRangePickerInput({
  startDate,
  endDate,
  onStartDateChange,
  onEndDateChange,
}: DateRangePickerInputProps) {
  const [isOpen, setIsOpen] = React.useState(false)
  // Temporary state for the modal
  const [tempStart, setTempStart] = React.useState<Date | undefined>(() =>
    startDate ? new Date(startDate) : undefined
  )
  const [tempEnd, setTempEnd] = React.useState<Date | undefined>(() =>
    endDate ? new Date(endDate) : undefined
  )

  const start = startDate ? new Date(startDate) : undefined
  const end = endDate ? new Date(endDate) : undefined

  const handleOpen = () => {
    // Reset temp values when opening
    setTempStart(start)
    setTempEnd(end)
    setIsOpen(true)
  }

  const handleCancel = () => {
    setIsOpen(false)
  }

  const handleApply = () => {
    if (tempStart) {
      onStartDateChange(format(tempStart, 'yyyy-MM-dd'))
    }
    if (tempEnd) {
      onEndDateChange(format(tempEnd, 'yyyy-MM-dd'))
    }
    setIsOpen(false)
  }

  const displayValue = () => {
    if (start && end) {
      return `${format(start, 'MMMM dd, yyyy')} → ${format(end, 'MMMM dd, yyyy')}`
    }
    if (start) {
      return format(start, 'MMMM dd, yyyy')
    }
    return "Select date range"
  }

  return (
    <>
      <button
        type="button"
        onClick={handleOpen}
        className={cn(
          "w-full flex items-center justify-between px-3 py-2 text-sm",
          "border rounded-md bg-background",
          "hover:bg-accent/50 transition-colors",
          "text-left"
        )}
      >
        <span className={!start ? "text-muted-foreground" : ""}>
          {displayValue()}
        </span>
        <ChevronRight className={cn("h-4 w-4 transition-transform", isOpen && "rotate-90")} />
      </button>

      {/* Modal Backdrop */}
      {isOpen && (
        <div className="fixed inset-0 z-50 flex items-center justify-center">
          {/* Backdrop */}
          <div
            className="absolute inset-0 bg-black/50"
            onClick={handleCancel}
          />

          {/* Modal Content */}
          <div className="relative z-50 bg-background rounded-lg shadow-xl">
            {/* Calendars and Presets */}
            <DateRangePicker
              startDate={tempStart}
              endDate={tempEnd}
              onStartDateChange={setTempStart}
              onEndDateChange={setTempEnd}
              className="border-0 shadow-none"
            />

            {/* Action Buttons */}
            <div className="flex justify-end gap-2 p-4 border-t">
              <Button variant="outline" onClick={handleCancel}>
                Cancel
              </Button>
              <Button onClick={handleApply}>
                Apply
              </Button>
            </div>
          </div>
        </div>
      )}
    </>
  )
}
