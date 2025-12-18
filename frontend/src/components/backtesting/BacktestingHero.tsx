import { Button } from '@/components/ui/button';
import { RotateCcw, Play } from 'lucide-react';

interface BacktestingHeroProps {
  onCreateSession?: () => void;
  onWatchTutorial?: () => void;
}

export function BacktestingHero({ onCreateSession, onWatchTutorial }: BacktestingHeroProps) {
  return (
    <div className="relative overflow-hidden rounded-lg bg-slate-900 min-h-[300px]">
      {/* Background pattern - simulated chart/candlestick visualization */}
      <div className="absolute inset-0 opacity-20">
        <div className="absolute inset-0 bg-gradient-to-br from-slate-800 via-slate-900 to-slate-950" />
        {/* Simulated candlestick pattern using CSS */}
        <div className="absolute inset-0" style={{
          backgroundImage: `
            linear-gradient(90deg, transparent 48%, rgba(34, 197, 94, 0.3) 48%, rgba(34, 197, 94, 0.3) 52%, transparent 52%),
            linear-gradient(90deg, transparent 18%, rgba(239, 68, 68, 0.3) 18%, rgba(239, 68, 68, 0.3) 22%, transparent 22%),
            linear-gradient(90deg, transparent 78%, rgba(34, 197, 94, 0.3) 78%, rgba(34, 197, 94, 0.3) 82%, transparent 82%)
          `,
          backgroundSize: '100px 100%',
        }} />
        {/* Grid lines */}
        <div className="absolute inset-0" style={{
          backgroundImage: `
            linear-gradient(to right, rgba(148, 163, 184, 0.1) 1px, transparent 1px),
            linear-gradient(to bottom, rgba(148, 163, 184, 0.1) 1px, transparent 1px)
          `,
          backgroundSize: '50px 50px',
        }} />
      </div>

      {/* Content */}
      <div className="relative z-10 flex flex-col items-center justify-center py-16 px-6 text-center">
        {/* Icon */}
        <div className="w-20 h-20 rounded-full bg-amber-500 flex items-center justify-center mb-6">
          <RotateCcw className="w-10 h-10 text-white" />
        </div>

        {/* Heading */}
        <h1 className="text-3xl font-bold text-white mb-8">
          Start Backtesting Now
        </h1>

        {/* CTA Button */}
        <Button
          onClick={onCreateSession}
          className="bg-amber-500 hover:bg-amber-600 text-white font-medium px-6 py-3 h-auto text-base mb-4"
        >
          + Create backtesting session
        </Button>

        {/* Tutorial Link */}
        <button
          onClick={onWatchTutorial}
          className="flex items-center gap-2 text-slate-300 hover:text-white transition-colors text-sm"
        >
          <Play className="w-4 h-4" />
          Watch tutorial video
        </button>
      </div>
    </div>
  );
}
