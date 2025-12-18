import { useState, useMemo } from 'react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Badge } from '@/components/ui/badge';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import {
  ArrowLeft,
  Settings,
  Calendar,
  Play,
  Pause,
  SkipBack,
  SkipForward,
  ChevronLeft,
  ChevronRight,
  Plus,
  Crosshair,
  TrendingUp,
  Type,
  Pencil,
  MousePointer,
  Maximize2,
  BarChart3,
  Clock,
} from 'lucide-react';
import type { SessionConfig } from './CreateSessionModal';
import { TradingViewChart, generateSampleCandleData } from './TradingViewChart';
import type { CandlestickData, Time } from 'lightweight-charts';

interface BacktestingSessionProps {
  session: SessionConfig & { id: string };
  onBack: () => void;
}

export function BacktestingSession({ session, onBack }: BacktestingSessionProps) {
  const [isPlaying, setIsPlaying] = useState(false);
  const [playbackSpeed, setPlaybackSpeed] = useState(1);
  const [timeframe, setTimeframe] = useState('1min');
  const [currentTime] = useState(new Date('2023-12-05T12:00:00'));
  const [selectedSymbol] = useState('US30');
  const [crosshairPrice, setCrosshairPrice] = useState<number | null>(null);

  // Order panel state
  const [positionSize, setPositionSize] = useState('0');
  const [profitTarget, setProfitTarget] = useState('0');
  const [stopLoss, setStopLoss] = useState('0');
  const [advancedOrder, setAdvancedOrder] = useState(false);

  // Generate chart data based on session date range
  const candleData = useMemo<CandlestickData<Time>[]>(() => {
    const startDate = session.dateRange.start
      ? new Date(session.dateRange.start)
      : new Date('2023-12-01');
    const endDate = session.dateRange.end
      ? new Date(session.dateRange.end)
      : new Date('2023-12-31');

    // Determine timeframe in minutes
    const timeframeMinutes = {
      '1min': 1,
      '5min': 5,
      '15min': 15,
      '1h': 60,
      '4h': 240,
      '1d': 1440,
    }[timeframe] || 1;

    return generateSampleCandleData(startDate, endDate, timeframeMinutes, 100);
  }, [session.dateRange.start, session.dateRange.end, timeframe]);

  const currentPrice = candleData.length > 0 ? candleData[candleData.length - 1].close : 100;
  const displayPrice = crosshairPrice ?? currentPrice;

  const handleCrosshairMove = (_time: Time | null, price: number | null) => {
    setCrosshairPrice(price);
  };

  const togglePlayback = () => {
    setIsPlaying(!isPlaying);
  };

  const formatDate = (date: Date) => {
    return date.toLocaleDateString('en-US', {
      weekday: 'short',
      month: 'short',
      day: '2-digit',
      year: 'numeric',
    });
  };

  const formatTime = (date: Date) => {
    return date.toLocaleTimeString('en-US', {
      hour: '2-digit',
      minute: '2-digit',
      hour12: true,
    });
  };

  return (
    <div className="flex-1 flex flex-col h-screen bg-background">
      {/* Top Header Bar */}
      <div className="flex items-center justify-between px-4 py-2 border-b bg-background">
        <div className="flex items-center gap-4">
          <Button variant="ghost" size="icon" onClick={onBack}>
            <ArrowLeft className="size-4" />
          </Button>
          <div className="flex items-center gap-2">
            <Select value={timeframe} onValueChange={setTimeframe}>
              <SelectTrigger className="w-16 h-8">
                <SelectValue />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="1min">1m</SelectItem>
                <SelectItem value="5min">5m</SelectItem>
                <SelectItem value="15min">15m</SelectItem>
                <SelectItem value="1h">1h</SelectItem>
                <SelectItem value="4h">4h</SelectItem>
                <SelectItem value="1d">1D</SelectItem>
              </SelectContent>
            </Select>
            <Button variant="ghost" size="sm" className="gap-1">
              <BarChart3 className="size-4" />
              Indicators
            </Button>
            <Button variant="ghost" size="sm">
              Go to
            </Button>
          </div>
        </div>

        <div className="flex items-center gap-2 text-sm">
          <span className="font-medium">{formatDate(currentTime)}</span>
          <span className="text-muted-foreground">-</span>
          <span>{formatTime(currentTime)}</span>
          <div className="w-48 h-1 bg-muted rounded-full ml-4">
            <div className="w-1/3 h-full bg-primary rounded-full" />
          </div>
        </div>

        <div className="flex items-center gap-2">
          <Button variant="ghost" size="icon">
            <Settings className="size-4" />
          </Button>
        </div>
      </div>

      {/* Main Content */}
      <div className="flex-1 flex overflow-hidden">
        {/* Left Toolbar */}
        <div className="w-12 border-r bg-background flex flex-col items-center py-2 gap-1">
          <Button variant="ghost" size="icon" className="size-8">
            <Plus className="size-4" />
          </Button>
          <Button variant="ghost" size="icon" className="size-8">
            <MousePointer className="size-4" />
          </Button>
          <Button variant="ghost" size="icon" className="size-8">
            <Crosshair className="size-4" />
          </Button>
          <Button variant="ghost" size="icon" className="size-8">
            <TrendingUp className="size-4" />
          </Button>
          <Button variant="ghost" size="icon" className="size-8">
            <Type className="size-4" />
          </Button>
          <Button variant="ghost" size="icon" className="size-8">
            <Pencil className="size-4" />
          </Button>
          <div className="flex-1" />
          <Button variant="ghost" size="icon" className="size-8">
            <Maximize2 className="size-4" />
          </Button>
        </div>

        {/* Chart Area */}
        <div className="flex-1 flex flex-col relative">
          {/* Symbol Info */}
          <div className="absolute top-2 left-4 z-10 flex items-center gap-2">
            <span className="font-semibold">{selectedSymbol} · 1</span>
            <Badge variant="outline" className="bg-green-500/20 text-green-500 border-green-500/30">
              ●
            </Badge>
            <span className="text-muted-foreground">···</span>
          </div>

          {/* TradingView Chart */}
          <div className="flex-1 bg-[#131722] relative overflow-hidden">
            <TradingViewChart
              data={candleData}
              onCrosshairMove={handleCrosshairMove}
            />
          </div>

          {/* Bottom Status Bar */}
          <div className="h-8 border-t bg-background flex items-center justify-between px-4 text-xs">
            <div className="flex items-center gap-4">
              <span className="text-muted-foreground">Date Range</span>
              <ChevronRight className="size-3" />
            </div>
            <div className="flex items-center gap-4 text-muted-foreground">
              <span>06:14:46 (UTC-5)</span>
              <span>%</span>
              <span>log</span>
              <span>auto</span>
            </div>
          </div>
        </div>

        {/* Right Panel - Order Entry */}
        <div className="w-72 border-l bg-background flex flex-col">
          {/* Order Panel Header */}
          <div className="p-4 border-b flex items-center justify-between">
            <h3 className="font-semibold">PLACE ORDER</h3>
            <Button variant="ghost" size="icon" className="size-6">
              <Settings className="size-4" />
            </Button>
          </div>

          {/* Order Form */}
          <div className="p-4 space-y-4 flex-1">
            {/* Advanced Order Toggle */}
            <div className="flex items-center justify-between">
              <span className="text-sm">Advanced order</span>
              <button
                onClick={() => setAdvancedOrder(!advancedOrder)}
                className={`w-10 h-5 rounded-full transition-colors ${
                  advancedOrder ? 'bg-primary' : 'bg-muted'
                }`}
              >
                <div
                  className={`w-4 h-4 rounded-full bg-white shadow transition-transform ${
                    advancedOrder ? 'translate-x-5' : 'translate-x-0.5'
                  }`}
                />
              </button>
            </div>

            {/* Position Size */}
            <div className="space-y-2">
              <Label className="text-muted-foreground text-xs">Position size</Label>
              <div className="flex gap-2">
                <Input
                  type="number"
                  value={positionSize}
                  onChange={(e) => setPositionSize(e.target.value)}
                  className="flex-1"
                />
                <Badge variant="outline" className="px-3">{selectedSymbol}</Badge>
              </div>
            </div>

            {/* Market Price */}
            <div className="space-y-2">
              <Label className="text-muted-foreground text-xs">Market price</Label>
              <div className="flex items-center gap-2">
                <Badge variant="outline" className="px-2">USD</Badge>
                <span className="font-mono">{displayPrice.toFixed(2)}</span>
              </div>
            </div>

            {/* Profit Target & Stop Loss */}
            <div className="grid grid-cols-2 gap-4">
              <div className="space-y-2">
                <Label className="text-muted-foreground text-xs">Profit target</Label>
                <div className="flex gap-1">
                  <Badge variant="outline" className="px-2 text-xs">USD</Badge>
                  <Input
                    type="number"
                    value={profitTarget}
                    onChange={(e) => setProfitTarget(e.target.value)}
                    className="flex-1"
                  />
                </div>
              </div>
              <div className="space-y-2">
                <Label className="text-muted-foreground text-xs">Stop loss</Label>
                <div className="flex gap-1">
                  <Badge variant="outline" className="px-2 text-xs">USD</Badge>
                  <Input
                    type="number"
                    value={stopLoss}
                    onChange={(e) => setStopLoss(e.target.value)}
                    className="flex-1"
                  />
                </div>
              </div>
            </div>

            {/* Reward & Risk */}
            <div className="grid grid-cols-2 gap-4 text-sm">
              <div>
                <span className="text-muted-foreground">Reward</span>
                <div className="text-green-500">0 USD</div>
              </div>
              <div className="text-right">
                <span className="text-red-500">Risk</span>
                <div>0 USD</div>
              </div>
            </div>

            {/* Total */}
            <div>
              <span className="text-muted-foreground text-sm">Total</span>
              <div className="font-mono">0 USD</div>
            </div>

            {/* Buy/Sell Buttons */}
            <div className="grid grid-cols-2 gap-2 pt-4">
              <Button className="bg-green-600 hover:bg-green-700">Buy</Button>
              <Button variant="secondary">Sell</Button>
            </div>
          </div>

          {/* Right Sidebar Icons */}
          <div className="border-t p-2 flex justify-around">
            <Button variant="ghost" size="icon" className="flex-col gap-1 h-auto py-2">
              <Plus className="size-4" />
              <span className="text-[10px]">Order</span>
            </Button>
            <Button variant="ghost" size="icon" className="flex-col gap-1 h-auto py-2">
              <BarChart3 className="size-4" />
              <span className="text-[10px]">Details</span>
            </Button>
            <Button variant="ghost" size="icon" className="flex-col gap-1 h-auto py-2">
              <Calendar className="size-4" />
              <span className="text-[10px]">Calendar</span>
            </Button>
            <Button variant="ghost" size="icon" className="flex-col gap-1 h-auto py-2">
              <Clock className="size-4" />
              <span className="text-[10px]">Orders</span>
            </Button>
          </div>
        </div>
      </div>

      {/* Bottom Playback Controls */}
      <div className="h-12 border-t bg-background flex items-center justify-center gap-4">
        <span className="text-sm text-muted-foreground">{playbackSpeed}x</span>
        <input
          type="range"
          min="0.25"
          max="4"
          step="0.25"
          value={playbackSpeed}
          onChange={(e) => setPlaybackSpeed(parseFloat(e.target.value))}
          className="w-32"
        />
        <div className="flex items-center gap-1">
          <Button variant="ghost" size="icon" className="size-8">
            <SkipBack className="size-4" />
          </Button>
          <Button variant="ghost" size="icon" className="size-8">
            <ChevronLeft className="size-4" />
          </Button>
          <Button
            variant="ghost"
            size="icon"
            className="size-10"
            onClick={togglePlayback}
          >
            {isPlaying ? <Pause className="size-5" /> : <Play className="size-5" />}
          </Button>
          <Button variant="ghost" size="icon" className="size-8">
            <ChevronRight className="size-4" />
          </Button>
          <Button variant="ghost" size="icon" className="size-8">
            <SkipForward className="size-4" />
          </Button>
        </div>
        <Select value={timeframe} onValueChange={setTimeframe}>
          <SelectTrigger className="w-20 h-8">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value="1min">1min</SelectItem>
            <SelectItem value="5min">5min</SelectItem>
            <SelectItem value="15min">15min</SelectItem>
          </SelectContent>
        </Select>
      </div>
    </div>
  );
}
