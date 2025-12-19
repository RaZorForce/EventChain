import { useState, useRef, useEffect } from 'react';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
  DialogDescription,
} from '@/components/ui/dialog';
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
  Save,
  MoreVertical,
  Globe,
  LayoutGrid,
  List,
  Upload,
  FileSpreadsheet,
  AlertCircle,
  Cloud,
  CloudOff,
} from 'lucide-react';
import { api, isBackendAvailable } from '@/lib/api';

interface Symbol {
  ticker: string;
  name: string;
  sector?: string;
  marketCap?: string;
}

interface SavedUniverse {
  id: string;
  name: string;
  description: string;
  symbols: Symbol[];
  createdAt: Date;
  source?: 'scanner' | 'csv';
  backendId?: string; // ID from backend storage
  syncedToBackend?: boolean;
}

interface UniverseManagerProps {
  onRequestHistoricalData?: (symbols: Symbol[]) => void;
}

export function UniverseManager({ onRequestHistoricalData }: UniverseManagerProps) {
  const [activeTab, setActiveTab] = useState<'my-universe' | 'scanner'>('scanner');
  const [viewMode, setViewMode] = useState<'list' | 'card'>('list');
  const [isScanning, setIsScanning] = useState(false);
  const [coarseUniverse, setCoarseUniverse] = useState<Symbol[]>([]);
  const [refinedUniverse, setRefinedUniverse] = useState<Symbol[]>([]);
  const [selectedCoarse, setSelectedCoarse] = useState<Set<string>>(new Set());
  const [selectedRefined, setSelectedRefined] = useState<Set<string>>(new Set());

  // LocalStorage key for persistence
  const STORAGE_KEY = 'eventchain-saved-universes';

  // Saved universes state - load from localStorage on init
  const [savedUniverses, setSavedUniverses] = useState<SavedUniverse[]>(() => {
    try {
      const stored = localStorage.getItem(STORAGE_KEY);
      if (stored) {
        const parsed = JSON.parse(stored);
        // Convert date strings back to Date objects
        return parsed.map((u: SavedUniverse & { createdAt: string }) => ({
          ...u,
          createdAt: new Date(u.createdAt),
        }));
      }
    } catch (error) {
      console.error('Failed to load saved universes from localStorage:', error);
    }
    return [];
  });
  const [showSaveDialog, setShowSaveDialog] = useState(false);
  const [universeName, setUniverseName] = useState('');
  const [universeDescription, setUniverseDescription] = useState('');

  // Persist saved universes to localStorage whenever they change
  useEffect(() => {
    try {
      localStorage.setItem(STORAGE_KEY, JSON.stringify(savedUniverses));
    } catch (error) {
      console.error('Failed to save universes to localStorage:', error);
    }
  }, [savedUniverses]);

  // Backend connectivity state
  const [backendAvailable, setBackendAvailable] = useState(false);
  const [isUploading, setIsUploading] = useState(false);

  // Check backend availability on mount and periodically
  useEffect(() => {
    const checkBackend = async () => {
      const available = await isBackendAvailable();
      setBackendAvailable(available);
    };

    checkBackend();
    const interval = setInterval(checkBackend, 30000); // Check every 30 seconds

    return () => clearInterval(interval);
  }, []);

  // CSV Import state
  const [showImportDialog, setShowImportDialog] = useState(false);
  const [importName, setImportName] = useState('');
  const [importDescription, setImportDescription] = useState('');
  const [csvFile, setCsvFile] = useState<File | null>(null);
  const [csvPreview, setCsvPreview] = useState<Symbol[]>([]);
  const [csvError, setCsvError] = useState<string | null>(null);
  const [isProcessingCsv, setIsProcessingCsv] = useState(false);
  const fileInputRef = useRef<HTMLInputElement>(null);

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

  const handleSaveUniverse = () => {
    if (!universeName.trim() || refinedUniverse.length === 0) return;

    const newUniverse: SavedUniverse = {
      id: Date.now().toString(),
      name: universeName,
      description: universeDescription,
      symbols: [...refinedUniverse],
      createdAt: new Date(),
    };

    setSavedUniverses([...savedUniverses, newUniverse]);
    setShowSaveDialog(false);
    setUniverseName('');
    setUniverseDescription('');

    // Switch to My Universe tab to show saved universe
    setActiveTab('my-universe');
  };

  const handleDeleteUniverse = async (id: string) => {
    const universe = savedUniverses.find((u) => u.id === id);

    // If synced to backend, try to delete from backend too
    if (universe?.backendId && backendAvailable) {
      try {
        await api.deleteUniverse(universe.backendId);
        console.log('Deleted from backend:', universe.backendId);
      } catch (error) {
        console.warn('Failed to delete from backend:', error);
        // Continue with local deletion anyway
      }
    }

    setSavedUniverses(savedUniverses.filter((u) => u.id !== id));
  };

  const handleLoadUniverse = (universe: SavedUniverse) => {
    setRefinedUniverse([...universe.symbols]);
    setActiveTab('scanner');
  };

  // CSV Import handlers
  type CsvFormat = 'symbol_list' | 'ohlcv' | 'unknown';

  const detectCsvFormat = (header: string[]): CsvFormat => {
    const headerLower = header.map(h => h.toLowerCase().trim());

    // Check for OHLCV format - requires Open, High, Low, Close
    const requiredOhlc = ['open', 'high', 'low', 'close'];
    if (requiredOhlc.every(col => headerLower.includes(col))) {
      return 'ohlcv';
    }

    // Check for symbol list format
    if (headerLower.some(h => h === 'ticker' || h === 'symbol')) {
      return 'symbol_list';
    }

    return 'unknown';
  };

  const parseFilenameInfo = (filename: string): { ticker: string; resolution: string } => {
    // Remove .csv extension
    const name = filename.replace(/\.csv$/i, '');

    // Resolution patterns
    const resolutionPatterns: Record<string, string[]> = {
      'daily': ['daily', 'day', '1d', 'd', 'bars'],
      '1m': ['1m', '1min', '1minute'],
      '5m': ['5m', '5min', '5minute'],
      '15m': ['15m', '15min', '15minute'],
      '30m': ['30m', '30min', '30minute'],
      '1h': ['1h', '1hr', '1hour', 'hourly'],
      '4h': ['4h', '4hr', '4hour'],
    };

    // Split by common delimiters
    const parts = name.replace(/-/g, '_').replace(/ /g, '_').split('_');

    let ticker = 'UNKNOWN';
    let resolution = 'daily';

    if (parts.length >= 1) {
      // First part is usually the ticker
      ticker = parts[0].toUpperCase().replace(/[^A-Z0-9]/g, '');

      // Look for resolution in remaining parts
      for (const part of parts.slice(1)) {
        const partLower = part.toLowerCase();
        for (const [res, patterns] of Object.entries(resolutionPatterns)) {
          if (patterns.some(p => partLower.includes(p))) {
            resolution = res;
            break;
          }
        }
      }
    }

    return { ticker, resolution };
  };

  const parseCsvContent = (content: string, filename: string): { symbols: Symbol[]; format: CsvFormat; ohlcvInfo?: { ticker: string; resolution: string; rowCount: number } } => {
    const lines = content.trim().split('\n');
    if (lines.length < 2) {
      throw new Error('CSV must have at least a header row and one data row');
    }

    const header = lines[0].split(',').map(h => h.trim());
    const format = detectCsvFormat(header);

    if (format === 'unknown') {
      throw new Error('Unknown CSV format. Expected either symbol list (ticker,name,...) or OHLCV (Date,Open,High,Low,Close,Volume)');
    }

    if (format === 'ohlcv') {
      // For OHLCV, extract ticker from filename
      const fileInfo = parseFilenameInfo(filename);
      const rowCount = lines.length - 1;

      // Create a single "symbol" entry representing this OHLCV data
      const symbols: Symbol[] = [{
        ticker: fileInfo.ticker,
        name: `${fileInfo.ticker} - ${fileInfo.resolution} data (${rowCount} bars)`,
        sector: 'Historical Data',
      }];

      return {
        symbols,
        format,
        ohlcvInfo: {
          ticker: fileInfo.ticker,
          resolution: fileInfo.resolution,
          rowCount
        }
      };
    }

    // Symbol list format
    const headerLower = header.map(h => h.toLowerCase().trim());
    const tickerIndex = headerLower.findIndex(h => h === 'ticker' || h === 'symbol');
    const nameIndex = headerLower.findIndex(h => h === 'name' || h === 'company');
    const sectorIndex = headerLower.findIndex(h => h === 'sector' || h === 'industry');
    const marketCapIndex = headerLower.findIndex(h => h === 'marketcap' || h === 'market_cap' || h === 'market cap');

    const symbols: Symbol[] = [];
    for (let i = 1; i < lines.length; i++) {
      const line = lines[i].trim();
      if (!line) continue;

      const values = line.split(',').map(v => v.trim().replace(/^"|"$/g, ''));
      const ticker = values[tickerIndex];

      if (ticker) {
        symbols.push({
          ticker: ticker.toUpperCase(),
          name: nameIndex >= 0 ? values[nameIndex] || ticker : ticker,
          sector: sectorIndex >= 0 ? values[sectorIndex] : undefined,
          marketCap: marketCapIndex >= 0 ? values[marketCapIndex] : undefined,
        });
      }
    }

    return { symbols, format };
  };

  // Track CSV format for display
  const [csvFormat, setCsvFormat] = useState<CsvFormat | null>(null);
  const [ohlcvInfo, setOhlcvInfo] = useState<{ ticker: string; resolution: string; rowCount: number } | null>(null);

  const handleFileSelect = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;

    setCsvFile(file);
    setCsvError(null);
    setIsProcessingCsv(true);
    setCsvFormat(null);
    setOhlcvInfo(null);

    try {
      const content = await file.text();
      const result = parseCsvContent(content, file.name);

      if (result.symbols.length === 0) {
        throw new Error('No valid symbols found in CSV');
      }

      setCsvPreview(result.symbols);
      setCsvFormat(result.format);
      if (result.ohlcvInfo) {
        setOhlcvInfo(result.ohlcvInfo);
      }
      // Auto-fill name from filename if empty
      if (!importName) {
        setImportName(file.name.replace(/\.csv$/i, ''));
      }
    } catch (error) {
      setCsvError(error instanceof Error ? error.message : 'Failed to parse CSV');
      setCsvPreview([]);
    } finally {
      setIsProcessingCsv(false);
    }
  };

  const handleImportCsv = async () => {
    if (!importName.trim() || csvPreview.length === 0 || !csvFile) return;

    setIsUploading(true);
    setCsvError(null);

    let backendId: string | undefined;
    let syncedToBackend = false;

    // Try to upload to backend if available
    if (backendAvailable) {
      try {
        const result = await api.uploadUniverse(csvFile, importName, importDescription);
        backendId = result.universe_id;
        syncedToBackend = true;
        console.log('Uploaded to backend:', result);
      } catch (error) {
        console.warn('Failed to upload to backend, saving locally only:', error);
        // Don't fail - just save locally
      }
    }

    const newUniverse: SavedUniverse = {
      id: Date.now().toString(),
      name: importName,
      description: importDescription,
      symbols: csvPreview,
      createdAt: new Date(),
      source: 'csv',
      backendId,
      syncedToBackend,
    };

    setSavedUniverses([...savedUniverses, newUniverse]);

    // Reset import state
    setShowImportDialog(false);
    setImportName('');
    setImportDescription('');
    setCsvFile(null);
    setCsvPreview([]);
    setCsvError(null);
    setIsUploading(false);
    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
  };

  const handleCloseImportDialog = () => {
    setShowImportDialog(false);
    setImportName('');
    setImportDescription('');
    setCsvFile(null);
    setCsvPreview([]);
    setCsvError(null);
    setCsvFormat(null);
    setOhlcvInfo(null);
    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
  };

  return (
    <div className="flex-1 flex flex-col min-h-screen overflow-auto">
      {/* Page Header */}
      <div className="py-4 px-6 border-b bg-background flex items-center justify-between">
        <div className="flex items-center gap-3">
          <h1 className="text-xl font-semibold">Universe</h1>
          {/* Backend status indicator */}
          <div
            className={`flex items-center gap-1.5 text-xs px-2 py-1 rounded-full ${
              backendAvailable
                ? 'bg-green-500/10 text-green-600'
                : 'bg-muted text-muted-foreground'
            }`}
            title={backendAvailable ? 'Backend connected - files will sync' : 'Backend offline - saving locally only'}
          >
            {backendAvailable ? (
              <>
                <Cloud className="size-3" />
                <span>Synced</span>
              </>
            ) : (
              <>
                <CloudOff className="size-3" />
                <span>Local only</span>
              </>
            )}
          </div>
        </div>
        <div className="flex items-center gap-2">
          {activeTab === 'scanner' && refinedUniverse.length > 0 && (
            <Button
              variant="outline"
              onClick={() => setShowSaveDialog(true)}
              className="gap-2"
            >
              <Save className="size-4" />
              Save Universe
            </Button>
          )}
          <Button
            onClick={handleRequestData}
            disabled={refinedUniverse.length === 0}
            className="gap-2"
          >
            <Download className="size-4" />
            Request Historical Data
          </Button>
        </div>
      </div>

      {/* Main Content with Tabs */}
      <div className="flex-1 flex flex-col bg-muted/20 p-6">
        <Card className="flex-1 flex flex-col">
          <Tabs value={activeTab} onValueChange={(v) => setActiveTab(v as 'my-universe' | 'scanner')} className="flex-1 flex flex-col">
            {/* Tabs and Actions Row */}
            <div className="flex items-center justify-between p-4 border-b">
              <TabsList className="bg-muted/50">
                <TabsTrigger value="my-universe">My Universe</TabsTrigger>
                <TabsTrigger value="scanner">Scanner</TabsTrigger>
              </TabsList>

              <div className="flex items-center gap-2">
                {activeTab === 'my-universe' && (
                  <Button
                    variant="outline"
                    size="sm"
                    onClick={() => setShowImportDialog(true)}
                    className="gap-1"
                  >
                    <Upload className="size-4" />
                    Import CSV
                  </Button>
                )}
                <button
                  onClick={() => setViewMode('list')}
                  className={`p-2 hover:bg-muted rounded-md transition-colors ${
                    viewMode === 'list' ? 'bg-muted text-foreground' : ''
                  }`}
                  title="List view"
                >
                  <List className="size-4 text-muted-foreground" />
                </button>
                <button
                  onClick={() => setViewMode('card')}
                  className={`p-2 hover:bg-muted rounded-md transition-colors ${
                    viewMode === 'card' ? 'bg-muted text-foreground' : ''
                  }`}
                  title="Card view"
                >
                  <LayoutGrid className="size-4 text-muted-foreground" />
                </button>
              </div>
            </div>

            {/* My Universe Tab */}
            <TabsContent value="my-universe" className="flex-1 m-0 overflow-auto">
              {savedUniverses.length === 0 ? (
                <div className="flex-1 flex items-center justify-center py-20">
                  <div className="text-center text-muted-foreground">
                    <Globe className="size-16 mx-auto mb-4 text-muted-foreground/50" />
                    <p className="text-lg font-medium mb-2">No saved universes yet</p>
                    <p className="text-sm mb-4">Use the Scanner tab to create a universe or import from CSV</p>
                    <div className="flex gap-2 justify-center">
                      <Button onClick={() => setActiveTab('scanner')} variant="outline" className="gap-2">
                        <Search className="size-4" />
                        Go to Scanner
                      </Button>
                      <Button onClick={() => setShowImportDialog(true)} variant="outline" className="gap-2">
                        <Upload className="size-4" />
                        Import CSV
                      </Button>
                    </div>
                  </div>
                </div>
              ) : viewMode === 'list' ? (
                // List View
                <>
                  {/* Table Header */}
                  <div className="grid grid-cols-[1fr_120px_120px_150px_40px] gap-4 px-4 py-3 border-b bg-muted/30 text-sm font-medium text-muted-foreground">
                    <div>Name</div>
                    <div className="text-right">Symbols</div>
                    <div className="text-right">Created</div>
                    <div className="text-right">Actions</div>
                    <div />
                  </div>

                  {/* Saved Universes List */}
                  <div className="divide-y">
                    {savedUniverses.map((universe) => (
                      <div
                        key={universe.id}
                        className="grid grid-cols-[1fr_120px_120px_150px_40px] gap-4 px-4 py-3 hover:bg-muted/20 transition-colors items-center"
                      >
                        <div>
                          <div className="font-medium text-foreground">{universe.name}</div>
                          <div className="text-xs text-muted-foreground">{universe.description || 'No description'}</div>
                        </div>
                        <div className="text-right text-muted-foreground">{universe.symbols.length}</div>
                        <div className="text-right text-muted-foreground text-sm">
                          {universe.createdAt.toLocaleDateString()}
                        </div>
                        <div className="flex justify-end gap-2">
                          <Button
                            variant="outline"
                            size="sm"
                            onClick={() => handleLoadUniverse(universe)}
                          >
                            Load
                          </Button>
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => handleDeleteUniverse(universe.id)}
                            className="text-destructive hover:text-destructive"
                          >
                            <X className="size-4" />
                          </Button>
                        </div>
                        <div className="flex justify-center">
                          <button className="p-1 hover:bg-muted rounded transition-colors">
                            <MoreVertical className="size-4 text-muted-foreground" />
                          </button>
                        </div>
                      </div>
                    ))}
                  </div>
                </>
              ) : (
                // Card View
                <div className="p-4">
                  <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
                    {savedUniverses.map((universe) => (
                      <Card key={universe.id} className="p-4 hover:bg-muted/20 transition-colors">
                        <div className="flex items-start justify-between mb-3">
                          <div className="flex-1">
                            <h3 className="font-medium text-foreground mb-1">{universe.name}</h3>
                            <p className="text-xs text-muted-foreground line-clamp-2">
                              {universe.description || 'No description'}
                            </p>
                          </div>
                          <button className="p-1 hover:bg-muted rounded transition-colors ml-2">
                            <MoreVertical className="size-4 text-muted-foreground" />
                          </button>
                        </div>

                        <div className="grid grid-cols-2 gap-2 text-xs mb-4">
                          <div>
                            <span className="text-muted-foreground">Symbols:</span>
                            <span className="ml-1 text-foreground">{universe.symbols.length}</span>
                          </div>
                          <div>
                            <span className="text-muted-foreground">Created:</span>
                            <span className="ml-1 text-foreground">
                              {universe.createdAt.toLocaleDateString()}
                            </span>
                          </div>
                        </div>

                        <div className="flex gap-2">
                          <Button
                            variant="outline"
                            size="sm"
                            className="flex-1"
                            onClick={() => handleLoadUniverse(universe)}
                          >
                            Load
                          </Button>
                          <Button
                            variant="ghost"
                            size="sm"
                            onClick={() => handleDeleteUniverse(universe.id)}
                            className="text-destructive hover:text-destructive"
                          >
                            <X className="size-4" />
                          </Button>
                        </div>
                      </Card>
                    ))}
                  </div>
                </div>
              )}
            </TabsContent>

            {/* Scanner Tab */}
            <TabsContent value="scanner" className="flex-1 m-0 p-4 flex flex-col gap-4 overflow-auto">
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
                {/* Scan Results */}
                <Card className="flex flex-col">
                  <div className="p-4 border-b flex items-center justify-between">
                    <h3 className="font-semibold">
                      Scan Results
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
                    title="Move all to tradeable"
                  >
                    <ChevronsRight className="size-4" />
                  </Button>
                  <Button
                    variant="outline"
                    size="icon"
                    onClick={handleMoveToRefined}
                    disabled={selectedCoarse.size === 0}
                    title="Move selected to tradeable"
                  >
                    <ChevronRight className="size-4" />
                  </Button>
                  <Button
                    variant="outline"
                    size="icon"
                    onClick={handleRemoveFromRefined}
                    disabled={selectedRefined.size === 0}
                    title="Remove selected from tradeable"
                  >
                    <ChevronLeft className="size-4" />
                  </Button>
                  <Button
                    variant="outline"
                    size="icon"
                    onClick={handleRemoveAllFromRefined}
                    disabled={refinedUniverse.length === 0}
                    title="Remove all from tradeable"
                  >
                    <ChevronsLeft className="size-4" />
                  </Button>
                </div>

                {/* Tradeable Universe */}
                <Card className="flex flex-col">
                  <div className="p-4 border-b flex items-center justify-between">
                    <h3 className="font-semibold">
                      Tradeable Universe
                      <span className="ml-2 text-sm font-normal text-muted-foreground">
                        ({refinedUniverse.length} symbols)
                      </span>
                    </h3>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={() => setShowSaveDialog(true)}
                      disabled={refinedUniverse.length === 0}
                      className="gap-1"
                    >
                      <Save className="size-4" />
                      Save
                    </Button>
                  </div>

                  <div className="flex-1 overflow-auto">
                    {refinedUniverse.length === 0 ? (
                      <div className="flex items-center justify-center h-full py-12 text-muted-foreground">
                        <div className="text-center">
                          <ChevronRight className="size-12 mx-auto mb-3 text-muted-foreground/50" />
                          <p className="text-sm">Move symbols from scan results</p>
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
            </TabsContent>
          </Tabs>
        </Card>
      </div>

      {/* Save Universe Dialog */}
      <Dialog open={showSaveDialog} onOpenChange={setShowSaveDialog}>
        <DialogContent className="sm:max-w-[425px]">
          <DialogHeader>
            <DialogTitle>Save Universe</DialogTitle>
          </DialogHeader>
          <div className="grid gap-4 py-4">
            <div className="space-y-2">
              <Label htmlFor="universe-name">
                Name<span className="text-destructive">*</span>
              </Label>
              <Input
                id="universe-name"
                placeholder="e.g. Tech Large Cap"
                value={universeName}
                onChange={(e) => setUniverseName(e.target.value)}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="universe-description">Description</Label>
              <Input
                id="universe-description"
                placeholder="Optional description"
                value={universeDescription}
                onChange={(e) => setUniverseDescription(e.target.value)}
              />
            </div>
            <div className="text-sm text-muted-foreground">
              This will save {refinedUniverse.length} symbols to your universe library.
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={() => setShowSaveDialog(false)}>
              Cancel
            </Button>
            <Button onClick={handleSaveUniverse} disabled={!universeName.trim()}>
              Save Universe
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      {/* Import CSV Dialog */}
      <Dialog open={showImportDialog} onOpenChange={(open) => !open && handleCloseImportDialog()}>
        <DialogContent className="sm:max-w-[550px]">
          <DialogHeader>
            <DialogTitle>Import Universe from CSV</DialogTitle>
            <DialogDescription>
              Upload a CSV file with your symbols. The file should have a "ticker" or "symbol" column.
            </DialogDescription>
          </DialogHeader>
          <div className="grid gap-4 py-4">
            {/* File Upload */}
            <div className="space-y-2">
              <Label>CSV File</Label>
              <div
                className="border-2 border-dashed rounded-lg p-6 text-center cursor-pointer hover:border-primary/50 transition-colors"
                onClick={() => fileInputRef.current?.click()}
              >
                <input
                  ref={fileInputRef}
                  type="file"
                  accept=".csv"
                  onChange={handleFileSelect}
                  className="hidden"
                />
                {isProcessingCsv ? (
                  <div className="flex flex-col items-center gap-2">
                    <Loader2 className="size-8 animate-spin text-muted-foreground" />
                    <p className="text-sm text-muted-foreground">Processing...</p>
                  </div>
                ) : csvFile ? (
                  <div className="flex flex-col items-center gap-2">
                    <FileSpreadsheet className="size-8 text-primary" />
                    <p className="text-sm font-medium">{csvFile.name}</p>
                    <p className="text-xs text-muted-foreground">Click to change file</p>
                  </div>
                ) : (
                  <div className="flex flex-col items-center gap-2">
                    <Upload className="size-8 text-muted-foreground" />
                    <p className="text-sm text-muted-foreground">
                      Click to upload or drag and drop
                    </p>
                    <p className="text-xs text-muted-foreground">CSV files only</p>
                  </div>
                )}
              </div>

              {csvError && (
                <div className="flex items-center gap-2 text-destructive text-sm">
                  <AlertCircle className="size-4" />
                  {csvError}
                </div>
              )}
            </div>

            {/* Preview */}
            {csvPreview.length > 0 && (
              <div className="space-y-2">
                {csvFormat === 'ohlcv' && ohlcvInfo ? (
                  <>
                    <Label>OHLCV Data Detected</Label>
                    <div className="border rounded-md p-4 bg-muted/30">
                      <div className="grid grid-cols-3 gap-4 text-sm">
                        <div>
                          <span className="text-muted-foreground">Ticker:</span>
                          <span className="ml-2 font-medium">{ohlcvInfo.ticker}</span>
                        </div>
                        <div>
                          <span className="text-muted-foreground">Resolution:</span>
                          <span className="ml-2 font-medium">{ohlcvInfo.resolution}</span>
                        </div>
                        <div>
                          <span className="text-muted-foreground">Bars:</span>
                          <span className="ml-2 font-medium">{ohlcvInfo.rowCount.toLocaleString()}</span>
                        </div>
                      </div>
                      <p className="text-xs text-muted-foreground mt-2">
                        This file will be saved to backend/data/Historical/ for backtesting.
                      </p>
                    </div>
                  </>
                ) : (
                  <>
                    <Label>Preview ({csvPreview.length} symbols)</Label>
                    <div className="border rounded-md max-h-40 overflow-auto">
                      <div className="divide-y">
                        {csvPreview.slice(0, 10).map((symbol) => (
                          <div key={symbol.ticker} className="px-3 py-2 flex items-center justify-between text-sm">
                            <div>
                              <span className="font-medium">{symbol.ticker}</span>
                              <span className="text-muted-foreground ml-2">{symbol.name}</span>
                            </div>
                            {symbol.sector && (
                              <span className="text-xs text-muted-foreground">{symbol.sector}</span>
                            )}
                          </div>
                        ))}
                        {csvPreview.length > 10 && (
                          <div className="px-3 py-2 text-sm text-muted-foreground text-center">
                            ... and {csvPreview.length - 10} more symbols
                          </div>
                        )}
                      </div>
                    </div>
                  </>
                )}
              </div>
            )}

            {/* Name and Description */}
            <div className="space-y-2">
              <Label htmlFor="import-name">
                Universe Name<span className="text-destructive">*</span>
              </Label>
              <Input
                id="import-name"
                placeholder="e.g. My Watchlist"
                value={importName}
                onChange={(e) => setImportName(e.target.value)}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="import-description">Description</Label>
              <Input
                id="import-description"
                placeholder="Optional description"
                value={importDescription}
                onChange={(e) => setImportDescription(e.target.value)}
              />
            </div>

            {/* Format Help */}
            <div className="text-xs text-muted-foreground bg-muted/50 p-3 rounded-md">
              <p className="font-medium mb-1">Supported CSV formats:</p>
              <div className="space-y-2 mt-2">
                <div>
                  <span className="font-medium">Symbol List:</span>
                  <code className="block ml-2">ticker,name,sector,marketcap</code>
                </div>
                <div>
                  <span className="font-medium">OHLCV Data:</span>
                  <code className="block ml-2">Date,Open,High,Low,Close,Volume</code>
                  <span className="block ml-2 text-muted-foreground/70">Ticker extracted from filename (e.g., AAPL_Daily.csv)</span>
                </div>
              </div>
            </div>
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={handleCloseImportDialog} disabled={isUploading}>
              Cancel
            </Button>
            <Button
              onClick={handleImportCsv}
              disabled={!importName.trim() || csvPreview.length === 0 || isUploading}
            >
              {isUploading ? (
                <>
                  <Loader2 className="size-4 mr-2 animate-spin" />
                  Uploading...
                </>
              ) : (
                <>
                  {backendAvailable && <Cloud className="size-4 mr-2" />}
                  {csvFormat === 'ohlcv' && ohlcvInfo
                    ? `Import ${ohlcvInfo.ticker} Data`
                    : csvPreview.length > 0
                      ? `Import ${csvPreview.length} Symbols`
                      : 'Import Universe'}
                </>
              )}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  );
}
