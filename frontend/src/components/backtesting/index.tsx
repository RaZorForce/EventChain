import { useState } from 'react';
import { BacktestingHero } from './BacktestingHero';
import { BacktestingFeatures } from './BacktestingFeatures';
import { StrategyLibrary } from './StrategyLibrary';
import { UniverseManager } from './UniverseManager';
import { CreateSessionModal, type SessionConfig } from './CreateSessionModal';
import { BacktestingSession } from './BacktestingSession';

// Dashboard Components (same as tracking dashboard)
import { DashboardHeader } from '@/components/dashboard/DashboardHeader';
import { KPICards } from '@/components/dashboard/KPICards';
import { TradingScoreCard, TradesAndTimePanel } from '@/components/dashboard/TradingScore';
import { CumulativePnLChart, DailyPnLBarChart } from '@/components/dashboard/Charts';
import { TradingCalendar } from '@/components/dashboard/TradingCalendar';

// Mock Data
import { mockDashboardData } from '@/lib/mock-data';

// Import BacktestView type from sidebar
import type { BacktestView } from '@/components/app-sidebar';

// Mock strategies and universes for now
const mockStrategies = [
  { id: '1', name: 'Double Top Strategy' },
  { id: '2', name: 'Morning Breakout' },
  { id: '3', name: 'Gap Fill Strategy' },
];

const mockUniverses = [
  { id: '1', name: 'Tech Large Cap' },
  { id: '2', name: 'S&P 500 Leaders' },
  { id: '3', name: 'High Volume Movers' },
];

interface BacktestingPageProps {
  /** Which view to show */
  view?: BacktestView;
}

interface ActiveSession extends SessionConfig {
  id: string;
}

export function BacktestingPage({ view = 'sessions' }: BacktestingPageProps) {
  const data = mockDashboardData;
  const [showCreateModal, setShowCreateModal] = useState(false);
  const [activeSession, setActiveSession] = useState<ActiveSession | null>(null);

  const handleWatchTutorial = () => {
    // TODO: Implement tutorial video modal or link
    console.log('Watch tutorial clicked');
  };

  const handleCreateSession = () => {
    setShowCreateModal(true);
  };

  const handleSessionCreated = (config: SessionConfig) => {
    const newSession: ActiveSession = {
      ...config,
      id: `session-${Date.now()}`,
    };
    setActiveSession(newSession);
  };

  const handleBackToSessions = () => {
    setActiveSession(null);
  };

  // If there's an active session, show the backtesting session view
  if (activeSession && view === 'sessions') {
    return (
      <BacktestingSession
        session={activeSession}
        onBack={handleBackToSessions}
      />
    );
  }

  // Sessions landing page (default)
  if (view === 'sessions') {
    return (
      <>
        <div className="flex-1 flex flex-col min-h-screen overflow-auto">
          {/* Page Header */}
          <div className="py-4 px-6 border-b bg-background">
            <h1 className="text-xl font-semibold">Backtesting sessions</h1>
          </div>

          {/* Main Content */}
          <div className="flex-1 flex flex-col bg-muted/20 p-6">
            {/* Hero Section */}
            <BacktestingHero
              onCreateSession={handleCreateSession}
              onWatchTutorial={handleWatchTutorial}
            />

            {/* Features Section */}
            <BacktestingFeatures />
          </div>
        </div>

        {/* Create Session Modal */}
        <CreateSessionModal
          open={showCreateModal}
          onOpenChange={setShowCreateModal}
          onCreateSession={handleSessionCreated}
          strategies={mockStrategies}
          universes={mockUniverses}
        />
      </>
    );
  }

  // Dashboard view (same as tracking dashboard)
  if (view === 'dashboard') {
    return (
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
    );
  }

  // Strategy library view
  if (view === 'strategy') {
    const handleCreateStrategy = () => {
      // TODO: Implement create strategy modal/flow
      console.log('Create strategy clicked');
    };

    return <StrategyLibrary onCreateStrategy={handleCreateStrategy} />;
  }

  // Universe manager view
  if (view === 'universe') {
    const handleRequestHistoricalData = (symbols: { ticker: string; name: string }[]) => {
      // TODO: Implement historical data request
      console.log('Requesting historical data for:', symbols);
    };

    return <UniverseManager onRequestHistoricalData={handleRequestHistoricalData} />;
  }

  // Other views (journal, trades, notebook, reports) - not implemented yet
  const viewLabels: Record<BacktestView, string> = {
    sessions: 'Sessions',
    dashboard: 'Dashboard',
    journal: 'Daily Journal',
    trades: 'Trades',
    notebook: 'Notebook',
    strategy: 'Strategy',
    universe: 'Universe',
    reports: 'Reports',
  };

  return (
    <div className="flex-1 flex items-center justify-center min-h-screen">
      <div className="text-center text-muted-foreground">
        <p className="text-lg font-medium mb-2">{viewLabels[view]} Coming Soon</p>
        <p className="text-sm">This backtesting feature is not yet implemented.</p>
      </div>
    </div>
  );
}

export { BacktestingHero } from './BacktestingHero';
export { BacktestingFeatures } from './BacktestingFeatures';
