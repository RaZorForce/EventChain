import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs';
import { ScrollArea } from '@/components/ui/scroll-area';
import type { Trade, Position } from '@/types/dashboard';

interface RecentTradesTableProps {
  trades: Trade[];
  positions: Position[];
}

function formatCurrency(value: number): string {
  return new Intl.NumberFormat('en-US', {
    style: 'currency',
    currency: 'USD',
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  }).format(value);
}

function formatDate(dateStr: string): string {
  const date = new Date(dateStr);
  return date.toLocaleDateString('en-US', { month: '2-digit', day: '2-digit', year: 'numeric' });
}

export function RecentTradesTable({ trades, positions }: RecentTradesTableProps) {
  return (
    <Tabs defaultValue="recent" className="w-full">
      <TabsList className="w-full grid grid-cols-2">
        <TabsTrigger value="recent">Recent trades</TabsTrigger>
        <TabsTrigger value="positions">Open positions</TabsTrigger>
      </TabsList>
      <TabsContent value="recent" className="mt-2">
        <ScrollArea className="h-[180px]">
          <table className="w-full text-sm">
            <thead>
              <tr className="text-muted-foreground text-xs border-b">
                <th className="text-left py-2 font-normal">Close Date</th>
                <th className="text-left py-2 font-normal">Symbol</th>
                <th className="text-right py-2 font-normal">Net P&L</th>
              </tr>
            </thead>
            <tbody>
              {trades.map((trade) => (
                <tr key={trade.id} className="border-b border-border/50">
                  <td className="py-2 text-muted-foreground">{formatDate(trade.closeDate)}</td>
                  <td className="py-2 font-medium">{trade.symbol}</td>
                  <td className={`py-2 text-right ${trade.netPnL >= 0 ? 'text-profit' : 'text-loss'}`}>
                    {formatCurrency(trade.netPnL)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </ScrollArea>
      </TabsContent>
      <TabsContent value="positions" className="mt-2">
        <ScrollArea className="h-[180px]">
          {positions.length === 0 ? (
            <div className="flex items-center justify-center h-full text-muted-foreground text-sm">
              No open positions
            </div>
          ) : (
            <table className="w-full text-sm">
              <thead>
                <tr className="text-muted-foreground text-xs border-b">
                  <th className="text-left py-2 font-normal">Symbol</th>
                  <th className="text-left py-2 font-normal">Side</th>
                  <th className="text-right py-2 font-normal">Qty</th>
                  <th className="text-right py-2 font-normal">Unrealized P&L</th>
                </tr>
              </thead>
              <tbody>
                {positions.map((position) => (
                  <tr key={position.id} className="border-b border-border/50">
                    <td className="py-2 font-medium">{position.symbol}</td>
                    <td className={`py-2 ${position.side === 'LONG' ? 'text-profit' : 'text-loss'}`}>
                      {position.side}
                    </td>
                    <td className="py-2 text-right">{position.quantity}</td>
                    <td className={`py-2 text-right ${position.unrealizedPnL >= 0 ? 'text-profit' : 'text-loss'}`}>
                      {formatCurrency(position.unrealizedPnL)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </ScrollArea>
      </TabsContent>
    </Tabs>
  );
}
