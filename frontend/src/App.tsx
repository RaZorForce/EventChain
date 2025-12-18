import { useState } from "react"
import { AppSidebar, type ActiveTab, type BacktestView } from "@/components/app-sidebar"

// Dashboard Components
import { DashboardHeader } from "@/components/dashboard/DashboardHeader"
import { KPICards } from "@/components/dashboard/KPICards"
import { TradingScoreCard, TradesAndTimePanel } from "@/components/dashboard/TradingScore"
import { CumulativePnLChart, DailyPnLBarChart } from "@/components/dashboard/Charts"
import { TradingCalendar } from "@/components/dashboard/TradingCalendar"

// Backtesting Components
import { BacktestingPage } from "@/components/backtesting"

// Mock Data
import { mockDashboardData } from "@/lib/mock-data"

export default function App() {
  const [activeTab, setActiveTab] = useState<ActiveTab>("track")
  const [backtestView, setBacktestView] = useState<BacktestView>("sessions")
  const data = mockDashboardData;


  return (
    <div className="min-h-screen bg-background text-foreground flex">
      {/* Sidebar */}
      <AppSidebar
        activeTab={activeTab}
        onTabChange={setActiveTab}
        backtestView={backtestView}
        onBacktestViewChange={setBacktestView}
      />

      {/* Main Content - Conditional rendering based on active tab */}
      {activeTab === "track" && (
        <div className="flex-1 flex flex-col min-h-screen overflow-auto">
          {/* Dashboard Header */}
          <DashboardHeader lastImport={data.lastImport} />

          {/* Main Dashboard Content */}
          <div className="flex-1 flex flex-col gap-4 p-4 bg-muted/20">
            {/* KPI Cards Row */}
            <KPICards kpis={data.kpis} />

            {/* Charts Row with Trading Score */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
              {/* Column 1: Cumulative P&L Chart */}
              <div>
                <CumulativePnLChart data={data.cumulativePnL} />
              </div>

              {/* Column 2: Daily P&L Chart */}
              <div>
                <DailyPnLBarChart data={data.dailyPnL} />
              </div>

              {/* Column 3: Trading Score Card */}
              <div>
                <TradingScoreCard tradingScore={data.tradingScore} />
              </div>
            </div>

            {/* Bottom Section - Two Column Layout */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
              {/* Column 1: Recent Trades, Open Positions & Trade Time - 1/3 width */}
              <div className="lg:col-span-1">
                <TradesAndTimePanel
                  recentTrades={data.recentTrades}
                  openPositions={data.openPositions}
                  tradeTimeData={data.tradeTimeDistribution}
                />
              </div>

              {/* Column 2: Trading Calendar - 2/3 width */}
              <div className="lg:col-span-2">
                <TradingCalendar
                  calendarData={data.calendarData}
                  initialDate={new Date(2025, 1, 1)} // February 2025
                />
              </div>
            </div>
          </div>
        </div>
      )}

      {activeTab === "backtest" && (
        <BacktestingPage view={backtestView} />
      )}

      {/* Other tabs - placeholder */}
      {activeTab !== "track" && activeTab !== "backtest" && (
        <div className="flex-1 flex items-center justify-center min-h-screen">
          <div className="text-center text-muted-foreground">
            <p className="text-lg font-medium mb-2">Module Coming Soon</p>
            <p className="text-sm">This feature is not yet implemented.</p>
          </div>
        </div>
      )}
    </div>
  )
}
