import { useState, useEffect } from 'react';
import { BacktestingHero } from './BacktestingHero';
import { BacktestingFeatures } from './BacktestingFeatures';
import { StrategyLibrary, templateStrategies, STRATEGY_STORAGE_KEY, type SavedStrategy } from './StrategyLibrary';
import { UniverseManager } from './UniverseManager';
import { CreateSessionModal, type SessionConfig } from './CreateSessionModal';
import { BacktestingSession } from './BacktestingSession';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import { Badge } from '@/components/ui/badge';
import { Play, Trash2, Calendar, DollarSign, BarChart3 } from 'lucide-react';

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


// LocalStorage keys
const UNIVERSE_STORAGE_KEY = 'eventchain-saved-universes';
const SESSION_STORAGE_KEY = 'eventchain-saved-sessions';

interface SavedUniverse {
  id: string;
  name: string;
  description: string;
  symbols: { ticker: string; name: string }[];
  createdAt: string;
  source?: 'scanner' | 'csv';
}

interface SavedSession {
  id: string;
  name: string;
  description: string;
  strategy: string | null;
  strategyName?: string;
  universe: string | null;
  universeName?: string;
  startBalance: number;
  dateRange: {
    start: string;
    end: string;
  };
  createdAt: string;
  status: 'active' | 'completed' | 'paused';
}

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

  // Load saved universes from localStorage
  const [savedUniverses, setSavedUniverses] = useState<{ id: string; name: string }[]>([]);

  // Load saved strategies from localStorage
  const [savedStrategies, setSavedStrategies] = useState<{ id: string; name: string }[]>([]);

  // Load saved sessions from localStorage
  const [savedSessions, setSavedSessions] = useState<SavedSession[]>([]);

  // Reload universes and strategies when modal opens or view changes
  useEffect(() => {
    const loadUniverses = () => {
      try {
        const stored = localStorage.getItem(UNIVERSE_STORAGE_KEY);
        if (stored) {
          const parsed = JSON.parse(stored) as SavedUniverse[];
          setSavedUniverses(parsed.map(u => ({ id: u.id, name: u.name })));
        }
      } catch (error) {
        console.error('Failed to load universes:', error);
      }
    };

    const loadStrategies = () => {
      try {
        const stored = localStorage.getItem(STRATEGY_STORAGE_KEY);
        const userStrategies: { id: string; name: string }[] = [];
        if (stored) {
          const parsed = JSON.parse(stored) as SavedStrategy[];
          userStrategies.push(...parsed.map(s => ({ id: s.id, name: s.name })));
        }
        // Combine user strategies with template strategies
        const allStrategies = [
          ...userStrategies,
          ...templateStrategies.map(t => ({ id: t.id, name: t.name })),
        ];
        setSavedStrategies(allStrategies);
      } catch (error) {
        console.error('Failed to load strategies:', error);
        // Fall back to just template strategies
        setSavedStrategies(templateStrategies.map(t => ({ id: t.id, name: t.name })));
      }
    };

    const loadSessions = () => {
      try {
        const stored = localStorage.getItem(SESSION_STORAGE_KEY);
        if (stored) {
          const parsed = JSON.parse(stored) as SavedSession[];
          setSavedSessions(parsed);
        }
      } catch (error) {
        console.error('Failed to load sessions:', error);
      }
    };

    loadUniverses();
    loadStrategies();
    loadSessions();
  }, [showCreateModal, view]);

  const handleWatchTutorial = () => {
    // TODO: Implement tutorial video modal or link
    console.log('Watch tutorial clicked');
  };

  const handleCreateSession = () => {
    setShowCreateModal(true);
  };

  const handleSessionCreated = (config: SessionConfig) => {
    const sessionId = `session-${Date.now()}`;
    const newSession: ActiveSession = {
      ...config,
      id: sessionId,
    };

    // Find strategy and universe names for display
    const strategyName = savedStrategies.find(s => s.id === config.strategy)?.name;
    const universeName = savedUniverses.find(u => u.id === config.universe)?.name;

    // Save to localStorage
    const savedSession: SavedSession = {
      id: sessionId,
      name: config.name,
      description: config.description,
      strategy: config.strategy,
      strategyName,
      universe: config.universe,
      universeName,
      startBalance: config.startBalance,
      dateRange: config.dateRange,
      createdAt: new Date().toISOString(),
      status: 'active',
    };

    const existingSessions = [...savedSessions];
    existingSessions.unshift(savedSession);
    setSavedSessions(existingSessions);
    localStorage.setItem(SESSION_STORAGE_KEY, JSON.stringify(existingSessions));

    setActiveSession(newSession);
  };

  const handleBackToSessions = () => {
    setActiveSession(null);
  };

  const handleResumeSession = (session: SavedSession) => {
    const activeSession: ActiveSession = {
      id: session.id,
      name: session.name,
      description: session.description,
      strategy: session.strategy,
      universe: session.universe,
      startBalance: session.startBalance,
      dateRange: session.dateRange,
    };
    setActiveSession(activeSession);
  };

  const handleDeleteSession = (sessionId: string) => {
    const updatedSessions = savedSessions.filter(s => s.id !== sessionId);
    setSavedSessions(updatedSessions);
    localStorage.setItem(SESSION_STORAGE_KEY, JSON.stringify(updatedSessions));
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
    const formatDate = (dateStr: string | null) => {
      if (!dateStr) return 'N/A';
      return new Date(dateStr).toLocaleDateString('en-US', {
        month: 'short',
        day: 'numeric',
        year: 'numeric',
      });
    };

    return (
      <>
        <div className="flex-1 flex flex-col min-h-screen overflow-auto">
          {/* Page Header */}
          <div className="py-4 px-6 border-b bg-background">
            <h1 className="text-xl font-semibold">Backtesting sessions</h1>
          </div>

          {/* Main Content */}
          <div className="flex-1 flex flex-col bg-muted/20 p-6 gap-6">
            {/* My Sessions Section */}
            {savedSessions.length > 0 && (
              <Card className="p-6">
                <div className="flex items-center justify-between mb-4">
                  <h2 className="text-lg font-semibold">My Sessions</h2>
                  <Badge variant="secondary">{savedSessions.length} session{savedSessions.length !== 1 ? 's' : ''}</Badge>
                </div>
                <div className="divide-y">
                  {savedSessions.map((session) => (
                    <div
                      key={session.id}
                      className="py-4 first:pt-0 last:pb-0 flex items-center justify-between gap-4"
                    >
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2 mb-1">
                          <h3 className="font-medium truncate">{session.name}</h3>
                          <Badge
                            variant={session.status === 'active' ? 'default' : session.status === 'completed' ? 'secondary' : 'outline'}
                            className="text-xs"
                          >
                            {session.status}
                          </Badge>
                        </div>
                        <div className="flex items-center gap-4 text-sm text-muted-foreground">
                          {session.strategyName && (
                            <span className="flex items-center gap-1">
                              <BarChart3 className="size-3" />
                              {session.strategyName}
                            </span>
                          )}
                          <span className="flex items-center gap-1">
                            <DollarSign className="size-3" />
                            ${session.startBalance.toLocaleString()}
                          </span>
                          <span className="flex items-center gap-1">
                            <Calendar className="size-3" />
                            {formatDate(session.dateRange.start)} - {formatDate(session.dateRange.end)}
                          </span>
                        </div>
                      </div>
                      <div className="flex items-center gap-2">
                        <Button
                          size="sm"
                          onClick={() => handleResumeSession(session)}
                          className="gap-1"
                        >
                          <Play className="size-3" />
                          Resume
                        </Button>
                        <Button
                          size="sm"
                          variant="ghost"
                          onClick={() => handleDeleteSession(session.id)}
                          className="text-destructive hover:text-destructive hover:bg-destructive/10"
                        >
                          <Trash2 className="size-4" />
                        </Button>
                      </div>
                    </div>
                  ))}
                </div>
              </Card>
            )}

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
          strategies={savedStrategies}
          universes={savedUniverses}
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
