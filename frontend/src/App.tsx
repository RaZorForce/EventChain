import { AppSidebar } from "@/components/app-sidebar"
import {
  SidebarInset,
  SidebarProvider,
} from "@/components/ui/sidebar"
import React from "react"

// Dashboard Components
import { DashboardHeader } from "@/components/dashboard/DashboardHeader"
import { KPICards } from "@/components/dashboard/KPICards"
import { ZellaScoreCard, TradesAndTimePanel } from "@/components/dashboard/ZellaScore"
import { CumulativePnLChart, DailyPnLBarChart } from "@/components/dashboard/Charts"
import { TradingCalendar } from "@/components/dashboard/TradingCalendar"

// Mock Data
import { mockDashboardData } from "@/lib/mock-data"

export default function App() {
  const data = mockDashboardData;

  return (
    <div className="min-h-screen bg-background text-foreground flex">
      <SidebarProvider
        style={{
          "--sidebar-width": "350px",
          "--sidebar-width-icon": "3rem",
        } as React.CSSProperties}
      >
        <AppSidebar />
        <SidebarInset>
          {/* Dashboard Header */}
          <DashboardHeader lastImport={data.lastImport} />

          {/* Main Dashboard Content */}
          <div className="flex flex-1 flex-col gap-4 p-4 bg-muted/20">
            {/* KPI Cards Row */}
            <KPICards kpis={data.kpis} />

            {/* Charts Row with Zella Score */}
            <div className="grid grid-cols-1 lg:grid-cols-3 gap-4">
              {/* Column 1: Cumulative P&L Chart */}
              <div>
                <CumulativePnLChart data={data.cumulativePnL} />
              </div>

              {/* Column 2: Daily P&L Chart */}
              <div>
                <DailyPnLBarChart data={data.dailyPnL} />
              </div>

              {/* Column 3: Zella Score Card */}
              <div>
                <ZellaScoreCard zellaScore={data.zellaScore} />
              </div>
            </div>

            {/* Bottom Section - Two Column Layout */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
              {/* Column 1: Recent Trades, Open Positions & Trade Time */}
              <div>
                <TradesAndTimePanel
                  recentTrades={data.recentTrades}
                  openPositions={data.openPositions}
                  tradeTimeData={data.tradeTimeDistribution}
                />
              </div>

              {/* Column 2: Trading Calendar */}
              <div>
                <TradingCalendar
                  calendarData={data.calendarData}
                  initialDate={new Date(2025, 1, 1)} // February 2025
                />
              </div>
            </div>
          </div>
        </SidebarInset>
      </SidebarProvider>
    </div>
  )
}
