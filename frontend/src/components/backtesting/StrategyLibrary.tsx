import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Card } from '@/components/ui/card';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { Plus, Settings, LayoutGrid, List, MoreVertical } from 'lucide-react';

// Template strategy patterns (chart patterns)
const templateStrategies = [
  {
    name: 'Double Top',
    description: 'Bearish reversal pattern with two peaks at similar price levels',
  },
  {
    name: 'Double Bottom',
    description: 'Bullish reversal pattern with two troughs at similar price levels',
  },
  {
    name: 'Head and Shoulders',
    description: 'Bearish reversal pattern with three peaks, middle being highest',
  },
  {
    name: 'Inverse Head and Shoulders',
    description: 'Bullish reversal pattern with three troughs, middle being lowest',
  },
  {
    name: 'Cup and Handle',
    description: 'Bullish continuation pattern resembling a cup with a handle',
  },
  {
    name: 'Triangle (Ascending/Descending)',
    description: 'Continuation or reversal pattern with converging trendlines',
  },
];

interface StrategyLibraryProps {
  onCreateStrategy?: () => void;
}

export function StrategyLibrary({ onCreateStrategy }: StrategyLibraryProps) {
  const [viewMode, setViewMode] = useState<'list' | 'card'>('list');
  return (
    <div className="flex-1 flex flex-col min-h-screen overflow-auto">
      {/* Page Header */}
      <div className="py-4 px-6 border-b bg-background flex items-center justify-between">
        <h1 className="text-xl font-semibold">Strategy</h1>
      </div>

      {/* Main Content */}
      <div className="flex-1 flex flex-col bg-muted/20 p-6">
        <Card className="flex-1 flex flex-col">
          <Tabs defaultValue="my-strategy" className="flex-1 flex flex-col">
            {/* Tabs and Actions Row */}
            <div className="flex items-center justify-between p-4 border-b">
              <TabsList className="bg-muted/50">
                <TabsTrigger value="my-strategy">My Strategy</TabsTrigger>
                <TabsTrigger value="template-strategy">Template Strategy</TabsTrigger>
              </TabsList>

              <div className="flex items-center gap-2">
                <span className="text-sm text-muted-foreground">Learn more</span>
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
                <Button onClick={onCreateStrategy} className="gap-2">
                  <Plus className="size-4" />
                  Create Strategy
                </Button>
              </div>
            </div>

            {/* Table Content */}
            <div className="flex-1 overflow-auto">
              <TabsContent value="my-strategy" className="h-full m-0">
                {viewMode === 'list' ? (
                  // List View
                  <>
                    {/* Table Header */}
                    <div className="grid grid-cols-[auto_1fr_100px_100px_100px_120px_120px_120px_120px_40px] gap-4 px-4 py-3 border-b bg-muted/30 text-sm font-medium text-muted-foreground">
                      <div className="w-8">
                        <input type="checkbox" className="rounded border-muted-foreground/30" />
                      </div>
                      <div>Name</div>
                      <div className="text-right">Trades</div>
                      <div className="text-right">Net P&L</div>
                      <div className="text-right">Win Rate</div>
                      <div className="text-right">Missed Trades</div>
                      <div className="text-right">Expectancy</div>
                      <div className="text-right">Average Loser</div>
                      <div className="text-right">Average Winner</div>
                      <div className="flex justify-center">
                        <Settings className="size-4" />
                      </div>
                    </div>

                    {/* Empty State */}
                    <div className="flex-1 flex items-center justify-center py-20">
                      <div className="text-center text-muted-foreground">
                        <p className="text-lg font-medium mb-2">No strategies yet</p>
                        <p className="text-sm mb-4">Create your first strategy to start tracking performance</p>
                        <Button onClick={onCreateStrategy} variant="outline" className="gap-2">
                          <Plus className="size-4" />
                          Create Strategy
                        </Button>
                      </div>
                    </div>
                  </>
                ) : (
                  // Card View - Empty State
                  <div className="flex-1 flex items-center justify-center py-20">
                    <div className="text-center text-muted-foreground">
                      <LayoutGrid className="size-16 mx-auto mb-4 text-muted-foreground/50" />
                      <p className="text-lg font-medium mb-2">No strategies yet</p>
                      <p className="text-sm mb-4">Create your first strategy to start tracking performance</p>
                      <Button onClick={onCreateStrategy} variant="outline" className="gap-2">
                        <Plus className="size-4" />
                        Create Strategy
                      </Button>
                    </div>
                  </div>
                )}
              </TabsContent>

              <TabsContent value="template-strategy" className="h-full m-0">
                {viewMode === 'list' ? (
                  // List View
                  <>
                    {/* Table Header */}
                    <div className="grid grid-cols-[auto_1fr_100px_100px_100px_120px_120px_120px_120px_40px] gap-4 px-4 py-3 border-b bg-muted/30 text-sm font-medium text-muted-foreground">
                      <div className="w-8">
                        <input type="checkbox" className="rounded border-muted-foreground/30" />
                      </div>
                      <div>Name</div>
                      <div className="text-right">Trades</div>
                      <div className="text-right">Net P&L</div>
                      <div className="text-right">Win Rate</div>
                      <div className="text-right">Missed Trades</div>
                      <div className="text-right">Expectancy</div>
                      <div className="text-right">Average Loser</div>
                      <div className="text-right">Average Winner</div>
                      <div className="flex justify-center">
                        <Settings className="size-4" />
                      </div>
                    </div>

                    {/* Template Strategies List */}
                    <div className="divide-y">
                      {templateStrategies.map((strategy) => (
                        <div
                          key={strategy.name}
                          className="grid grid-cols-[auto_1fr_100px_100px_100px_120px_120px_120px_120px_40px] gap-4 px-4 py-3 hover:bg-muted/20 transition-colors items-center"
                        >
                          <div className="w-8">
                            <input type="checkbox" className="rounded border-muted-foreground/30" />
                          </div>
                          <div>
                            <div className="font-medium text-foreground">{strategy.name}</div>
                            <div className="text-xs text-muted-foreground">{strategy.description}</div>
                          </div>
                          <div className="text-right text-muted-foreground">0</div>
                          <div className="text-right text-muted-foreground">$0.00</div>
                          <div className="text-right text-muted-foreground">0.00%</div>
                          <div className="text-right text-muted-foreground">0</div>
                          <div className="text-right text-muted-foreground">$0.00</div>
                          <div className="text-right text-muted-foreground">$0.00</div>
                          <div className="text-right text-muted-foreground">$0.00</div>
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
                      {templateStrategies.map((strategy) => (
                        <Card key={strategy.name} className="p-4 hover:bg-muted/20 transition-colors cursor-pointer">
                          <div className="flex items-start justify-between mb-3">
                            <div className="flex-1">
                              <h3 className="font-medium text-foreground mb-1">{strategy.name}</h3>
                              <p className="text-xs text-muted-foreground line-clamp-2">{strategy.description}</p>
                            </div>
                            <button className="p-1 hover:bg-muted rounded transition-colors ml-2">
                              <MoreVertical className="size-4 text-muted-foreground" />
                            </button>
                          </div>
                          
                          <div className="grid grid-cols-2 gap-2 text-xs">
                            <div>
                              <span className="text-muted-foreground">Trades:</span>
                              <span className="ml-1 text-foreground">0</span>
                            </div>
                            <div>
                              <span className="text-muted-foreground">P&L:</span>
                              <span className="ml-1 text-foreground">$0.00</span>
                            </div>
                            <div>
                              <span className="text-muted-foreground">Win Rate:</span>
                              <span className="ml-1 text-foreground">0.00%</span>
                            </div>
                            <div>
                              <span className="text-muted-foreground">Expectancy:</span>
                              <span className="ml-1 text-foreground">$0.00</span>
                            </div>
                          </div>
                        </Card>
                      ))}
                    </div>
                  </div>
                )}
              </TabsContent>
            </div>
          </Tabs>
        </Card>
      </div>
    </div>
  );
}
