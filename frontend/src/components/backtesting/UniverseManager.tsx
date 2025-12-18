import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import {
  Search,
  Play,
  ChevronRight,
  ChevronLeft,
  ChevronsRight,
  ChevronsLeft,
  X,
  Download,
  Loader2,
} from 'lucide-react';

interface Symbol {
  ticker: string;
  name: string;
  sector?: string;
  marketCap?: string;
}

interface UniverseManagerProps {
  onRequestHistoricalData?: (symbols: Symbol[]) => void;
}

export function UniverseManager({ onRequestHistoricalData }: UniverseManagerProps) {
  const [isScanning, setIsScanning] = useState(false);
  const [coarseUniverse, setCoarseUniverse] = useState<Symbol[]>([]);
  const [refinedUniverse, setRefinedUniverse] = useState<Symbol[]>([]);
  const [selectedCoarse, setSelectedCoarse] = useState<Set<string>>(new Set());
  const [selectedRefined, setSelectedRefined] = useState<Set<string>>(new Set());

  // Scanner filter state
  const [filters, setFilters] = useState({
    exchange: '',
    sector: '',
    minMarketCap: '',
    maxMarketCap: '',
    minPrice: '',
    maxPrice: '',
    minVolume: '',
  });

  const handleRunScanner = async () => {
    setIsScanning(true);
    // TODO: Replace with actual API call
    // Simulating scanner results
    setTimeout(() => {
      const mockResults: Symbol[] = [
        { ticker: 'AAPL', name: 'Apple Inc.', sector: 'Technology', marketCap: '$3.0T' },
        { ticker: 'MSFT', name: 'Microsoft Corporation', sector: 'Technology', marketCap: '$2.8T' },
        { ticker: 'GOOGL', name: 'Alphabet Inc.', sector: 'Technology', marketCap: '$1.7T' },
        { ticker: 'AMZN', name: 'Amazon.com Inc.', sector: 'Consumer Cyclical', marketCap: '$1.5T' },
        { ticker: 'NVDA', name: 'NVIDIA Corporation', sector: 'Technology', marketCap: '$1.2T' },
        { ticker: 'META', name: 'Meta Platforms Inc.', sector: 'Technology', marketCap: '$900B' },
        { ticker: 'TSLA', name: 'Tesla Inc.', sector: 'Consumer Cyclical', marketCap: '$800B' },
        { ticker: 'JPM', name: 'JPMorgan Chase & Co.', sector: 'Financial', marketCap: '$500B' },
      ];
      setCoarseUniverse(mockResults);
      setIsScanning(false);
    }, 1500);
  };

  const handleMoveToRefined = () => {
    const symbolsToMove = coarseUniverse.filter((s) => selectedCoarse.has(s.ticker));
    const newRefined = [...refinedUniverse];
    symbolsToMove.forEach((symbol) => {
      if (!newRefined.find((s) => s.ticker === symbol.ticker)) {
        newRefined.push(symbol);
      }
    });
    setRefinedUniverse(newRefined);
    setSelectedCoarse(new Set());
  };

  const handleMoveAllToRefined = () => {
    const newRefined = [...refinedUniverse];
    coarseUniverse.forEach((symbol) => {
      if (!newRefined.find((s) => s.ticker === symbol.ticker)) {
        newRefined.push(symbol);
      }
    });
    setRefinedUniverse(newRefined);
  };

  const handleRemoveFromRefined = () => {
    const newRefined = refinedUniverse.filter((s) => !selectedRefined.has(s.ticker));
    setRefinedUniverse(newRefined);
    setSelectedRefined(new Set());
  };

  const handleRemoveAllFromRefined = () => {
    setRefinedUniverse([]);
    setSelectedRefined(new Set());
  };

  const handleRemoveSingleFromRefined = (ticker: string) => {
    setRefinedUniverse(refinedUniverse.filter((s) => s.ticker !== ticker));
    selectedRefined.delete(ticker);
    setSelectedRefined(new Set(selectedRefined));
  };

  const toggleCoarseSelection = (ticker: string) => {
    const newSelected = new Set(selectedCoarse);
    if (newSelected.has(ticker)) {
      newSelected.delete(ticker);
    } else {
      newSelected.add(ticker);
    }
    setSelectedCoarse(newSelected);
  };

  const toggleRefinedSelection = (ticker: string) => {
    const newSelected = new Set(selectedRefined);
    if (newSelected.has(ticker)) {
      newSelected.delete(ticker);
    } else {
      newSelected.add(ticker);
    }
    setSelectedRefined(newSelected);
  };

  const handleRequestData = () => {
    if (onRequestHistoricalData) {
      onRequestHistoricalData(refinedUniverse);
    }
    console.log('Requesting historical data for:', refinedUniverse);
  };

  return (
    <div className="flex-1 flex flex-col min-h-screen overflow-auto">
      {/* Page Header */}
      <div className="py-4 px-6 border-b bg-background flex items-center justify-between">
        <h1 className="text-xl font-semibold">Universe</h1>
        <Button
          onClick={handleRequestData}
          disabled={refinedUniverse.length === 0}
          className="gap-2"
        >
          <Download className="size-4" />
          Request Historical Data
        </Button>
      </div>

      {/* Main Content */}
      <div className="flex-1 flex flex-col bg-muted/20 p-6 gap-6">
        {/* Scanner Section */}
        <Card className="p-6">
          <h2 className="text-lg font-semibold mb-4 flex items-center gap-2">
            <Search className="size-5" />
            Symbol Scanner
          </h2>

          <div className="grid grid-cols-2 md:grid-cols-4 lg:grid-cols-7 gap-4 mb-4">
            <div className="space-y-2">
              <Label htmlFor="exchange">Exchange</Label>
              <Select
                value={filters.exchange}
                onValueChange={(value: string) => setFilters({ ...filters, exchange: value })}
              >
                <SelectTrigger id="exchange">
                  <SelectValue placeholder="All" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All</SelectItem>
                  <SelectItem value="NYSE">NYSE</SelectItem>
                  <SelectItem value="NASDAQ">NASDAQ</SelectItem>
                  <SelectItem value="AMEX">AMEX</SelectItem>
                </SelectContent>
              </Select>
            </div>

            <div className="space-y-2">
              <Label htmlFor="sector">Sector</Label>
              <Select
                value={filters.sector}
                onValueChange={(value: string) => setFilters({ ...filters, sector: value })}
              >
                <SelectTrigger id="sector">
                  <SelectValue placeholder="All" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All</SelectItem>
                  <SelectItem value="Technology">Technology</SelectItem>
                  <SelectItem value="Healthcare">Healthcare</SelectItem>
                  <SelectItem value="Financial">Financial</SelectItem>
                  <SelectItem value="Consumer Cyclical">Consumer Cyclical</SelectItem>
                  <SelectItem value="Energy">Energy</SelectItem>
                </SelectContent>
              </Select>
            </div>

            <div className="space-y-2">
              <Label htmlFor="minMarketCap">Min Market Cap</Label>
              <Input
                id="minMarketCap"
                placeholder="e.g. 1B"
                value={filters.minMarketCap}
                onChange={(e) => setFilters({ ...filters, minMarketCap: e.target.value })}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="maxMarketCap">Max Market Cap</Label>
              <Input
                id="maxMarketCap"
                placeholder="e.g. 100B"
                value={filters.maxMarketCap}
                onChange={(e) => setFilters({ ...filters, maxMarketCap: e.target.value })}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="minPrice">Min Price</Label>
              <Input
                id="minPrice"
                placeholder="e.g. 10"
                value={filters.minPrice}
                onChange={(e) => setFilters({ ...filters, minPrice: e.target.value })}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="maxPrice">Max Price</Label>
              <Input
                id="maxPrice"
                placeholder="e.g. 500"
                value={filters.maxPrice}
                onChange={(e) => setFilters({ ...filters, maxPrice: e.target.value })}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="minVolume">Min Avg Volume</Label>
              <Input
                id="minVolume"
                placeholder="e.g. 1M"
                value={filters.minVolume}
                onChange={(e) => setFilters({ ...filters, minVolume: e.target.value })}
              />
            </div>
          </div>

          <Button onClick={handleRunScanner} disabled={isScanning} className="gap-2">
            {isScanning ? (
              <>
                <Loader2 className="size-4 animate-spin" />
                Scanning...
              </>
            ) : (
              <>
                <Play className="size-4" />
                Run Scanner
              </>
            )}
          </Button>
        </Card>

        {/* Universe Management Section */}
        <div className="flex-1 grid grid-cols-[1fr_auto_1fr] gap-4">
          {/* Coarse Universe */}
          <Card className="flex flex-col">
            <div className="p-4 border-b flex items-center justify-between">
              <h3 className="font-semibold">
                Coarse Universe
                <span className="ml-2 text-sm font-normal text-muted-foreground">
                  ({coarseUniverse.length} symbols)
                </span>
              </h3>
            </div>

            <div className="flex-1 overflow-auto">
              {coarseUniverse.length === 0 ? (
                <div className="flex items-center justify-center h-full py-12 text-muted-foreground">
                  <div className="text-center">
                    <Search className="size-12 mx-auto mb-3 text-muted-foreground/50" />
                    <p className="text-sm">Run the scanner to populate symbols</p>
                  </div>
                </div>
              ) : (
                <div className="divide-y">
                  {coarseUniverse.map((symbol) => (
                    <div
                      key={symbol.ticker}
                      onClick={() => toggleCoarseSelection(symbol.ticker)}
                      className={`px-4 py-3 cursor-pointer transition-colors ${
                        selectedCoarse.has(symbol.ticker)
                          ? 'bg-primary/10 border-l-2 border-l-primary'
                          : 'hover:bg-muted/50'
                      }`}
                    >
                      <div className="flex items-center justify-between">
                        <div>
                          <div className="font-medium">{symbol.ticker}</div>
                          <div className="text-xs text-muted-foreground">{symbol.name}</div>
                        </div>
                        <div className="text-right text-xs text-muted-foreground">
                          <div>{symbol.sector}</div>
                          <div>{symbol.marketCap}</div>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </Card>

          {/* Transfer Controls */}
          <div className="flex flex-col items-center justify-center gap-2">
            <Button
              variant="outline"
              size="icon"
              onClick={handleMoveAllToRefined}
              disabled={coarseUniverse.length === 0}
              title="Move all to refined"
            >
              <ChevronsRight className="size-4" />
            </Button>
            <Button
              variant="outline"
              size="icon"
              onClick={handleMoveToRefined}
              disabled={selectedCoarse.size === 0}
              title="Move selected to refined"
            >
              <ChevronRight className="size-4" />
            </Button>
            <Button
              variant="outline"
              size="icon"
              onClick={handleRemoveFromRefined}
              disabled={selectedRefined.size === 0}
              title="Remove selected from refined"
            >
              <ChevronLeft className="size-4" />
            </Button>
            <Button
              variant="outline"
              size="icon"
              onClick={handleRemoveAllFromRefined}
              disabled={refinedUniverse.length === 0}
              title="Remove all from refined"
            >
              <ChevronsLeft className="size-4" />
            </Button>
          </div>

          {/* Refined Universe */}
          <Card className="flex flex-col">
            <div className="p-4 border-b flex items-center justify-between">
              <h3 className="font-semibold">
                Refined Universe
                <span className="ml-2 text-sm font-normal text-muted-foreground">
                  ({refinedUniverse.length} symbols)
                </span>
              </h3>
            </div>

            <div className="flex-1 overflow-auto">
              {refinedUniverse.length === 0 ? (
                <div className="flex items-center justify-center h-full py-12 text-muted-foreground">
                  <div className="text-center">
                    <ChevronRight className="size-12 mx-auto mb-3 text-muted-foreground/50" />
                    <p className="text-sm">Move symbols from coarse universe</p>
                  </div>
                </div>
              ) : (
                <div className="divide-y">
                  {refinedUniverse.map((symbol) => (
                    <div
                      key={symbol.ticker}
                      onClick={() => toggleRefinedSelection(symbol.ticker)}
                      className={`px-4 py-3 cursor-pointer transition-colors ${
                        selectedRefined.has(symbol.ticker)
                          ? 'bg-primary/10 border-l-2 border-l-primary'
                          : 'hover:bg-muted/50'
                      }`}
                    >
                      <div className="flex items-center justify-between">
                        <div>
                          <div className="font-medium">{symbol.ticker}</div>
                          <div className="text-xs text-muted-foreground">{symbol.name}</div>
                        </div>
                        <div className="flex items-center gap-2">
                          <div className="text-right text-xs text-muted-foreground">
                            <div>{symbol.sector}</div>
                            <div>{symbol.marketCap}</div>
                          </div>
                          <button
                            onClick={(e) => {
                              e.stopPropagation();
                              handleRemoveSingleFromRefined(symbol.ticker);
                            }}
                            className="p-1 hover:bg-destructive/20 rounded transition-colors"
                            title="Remove symbol"
                          >
                            <X className="size-4 text-muted-foreground hover:text-destructive" />
                          </button>
                        </div>
                      </div>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </Card>
        </div>
      </div>
    </div>
  );
}
