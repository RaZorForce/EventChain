import { useState } from 'react';
import { Button } from '@/components/ui/button';
import { Input } from '@/components/ui/input';
import { Label } from '@/components/ui/label';
import {
  Dialog,
  DialogContent,
  DialogHeader,
  DialogTitle,
  DialogFooter,
} from '@/components/ui/dialog';
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from '@/components/ui/select';
import { Badge } from '@/components/ui/badge';
import { X } from 'lucide-react';

export interface SessionConfig {
  name: string;
  description: string;
  strategy: string | null;
  universe: string | null;
  startBalance: number;
  dateRange: {
    start: string;
    end: string;
  };
}

interface CreateSessionModalProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onCreateSession: (config: SessionConfig) => void;
  strategies: { id: string; name: string }[];
  universes: { id: string; name: string }[];
}

export function CreateSessionModal({
  open,
  onOpenChange,
  onCreateSession,
  strategies,
  universes,
}: CreateSessionModalProps) {
  const [sessionName, setSessionName] = useState('');
  const [description, setDescription] = useState('');
  const [selectedStrategy, setSelectedStrategy] = useState<string | null>(null);
  const [selectedUniverse, setSelectedUniverse] = useState<string | null>(null);
  const [startBalance, setStartBalance] = useState('100000');
  const [startDate, setStartDate] = useState('');
  const [endDate, setEndDate] = useState('');

  const handleSubmit = () => {
    if (!sessionName || !selectedUniverse || !startDate || !endDate) {
      return;
    }

    onCreateSession({
      name: sessionName,
      description,
      strategy: selectedStrategy,
      universe: selectedUniverse,
      startBalance: parseFloat(startBalance) || 100000,
      dateRange: {
        start: startDate,
        end: endDate,
      },
    });

    // Reset form
    setSessionName('');
    setDescription('');
    setSelectedStrategy(null);
    setSelectedUniverse(null);
    setStartBalance('100000');
    setStartDate('');
    setEndDate('');
    onOpenChange(false);
  };

  const handleCancel = () => {
    onOpenChange(false);
  };

  const clearStrategy = () => {
    setSelectedStrategy(null);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-[500px]">
        <DialogHeader>
          <DialogTitle>Create new session</DialogTitle>
        </DialogHeader>

        <div className="grid gap-4 py-4">
          {/* Session Name */}
          <div className="space-y-2">
            <Label htmlFor="session-name">
              Session name<span className="text-destructive">*</span>
            </Label>
            <Input
              id="session-name"
              placeholder="Opening Range Test"
              value={sessionName}
              onChange={(e) => setSessionName(e.target.value)}
            />
          </div>

          {/* Description */}
          <div className="space-y-2">
            <Label htmlFor="description">Description</Label>
            <Input
              id="description"
              placeholder="We want to test the first 15 Mins of each session"
              value={description}
              onChange={(e) => setDescription(e.target.value)}
            />
          </div>

          {/* Connect to Strategy */}
          <div className="space-y-2">
            <div className="flex items-center justify-between">
              <Label>Connect to strategy</Label>
              <button
                type="button"
                className="text-xs text-primary hover:underline"
                onClick={() => {
                  // TODO: Open create strategy modal
                  console.log('Create new strategy');
                }}
              >
                Create new strategy
              </button>
            </div>
            {selectedStrategy ? (
              <div className="flex items-center gap-2">
                <Badge variant="secondary" className="gap-1 px-3 py-1.5">
                  {strategies.find((s) => s.id === selectedStrategy)?.name || selectedStrategy}
                  <button
                    type="button"
                    onClick={clearStrategy}
                    className="ml-1 hover:text-destructive"
                  >
                    <X className="size-3" />
                  </button>
                </Badge>
              </div>
            ) : (
              <Select
                value={selectedStrategy || ''}
                onValueChange={(value: string) => setSelectedStrategy(value)}
              >
                <SelectTrigger>
                  <SelectValue placeholder="Select a strategy (optional)" />
                </SelectTrigger>
                <SelectContent>
                  {strategies.length === 0 ? (
                    <div className="px-2 py-4 text-center text-sm text-muted-foreground">
                      No strategies available
                    </div>
                  ) : (
                    strategies.map((strategy) => (
                      <SelectItem key={strategy.id} value={strategy.id}>
                        {strategy.name}
                      </SelectItem>
                    ))
                  )}
                </SelectContent>
              </Select>
            )}
          </div>

          {/* Type - Only Stocks supported */}
          <div className="space-y-2">
            <Label>Type</Label>
            <div className="flex gap-2">
              <Badge variant="default" className="px-4 py-1.5">
                Stocks
              </Badge>
              <Badge variant="outline" className="px-4 py-1.5 opacity-50 cursor-not-allowed">
                Forex
              </Badge>
              <Badge variant="outline" className="px-4 py-1.5 opacity-50 cursor-not-allowed">
                Crypto
              </Badge>
              <Badge variant="outline" className="px-4 py-1.5 opacity-50 cursor-not-allowed">
                Futures
              </Badge>
            </div>
          </div>

          {/* Universe Selection */}
          <div className="space-y-2">
            <Label htmlFor="universe">
              Universe<span className="text-destructive">*</span>
            </Label>
            <Select
              value={selectedUniverse || ''}
              onValueChange={(value: string) => setSelectedUniverse(value)}
            >
              <SelectTrigger id="universe">
                <SelectValue placeholder="Select a universe" />
              </SelectTrigger>
              <SelectContent>
                {universes.length === 0 ? (
                  <div className="px-2 py-4 text-center text-sm text-muted-foreground">
                    No universes available. Create one in the Universe tab.
                  </div>
                ) : (
                  universes.map((universe) => (
                    <SelectItem key={universe.id} value={universe.id}>
                      {universe.name}
                    </SelectItem>
                  ))
                )}
              </SelectContent>
            </Select>
          </div>

          {/* Start Balance and Date Range */}
          <div className="grid grid-cols-2 gap-4">
            <div className="space-y-2">
              <Label htmlFor="start-balance">
                Start balance<span className="text-destructive">*</span>
              </Label>
              <div className="relative">
                <span className="absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground">
                  $
                </span>
                <Input
                  id="start-balance"
                  type="number"
                  className="pl-7"
                  value={startBalance}
                  onChange={(e) => setStartBalance(e.target.value)}
                />
              </div>
              <p className="text-xs text-muted-foreground">Leverage is 1:1</p>
            </div>

            <div className="space-y-2">
              <Label>
                Date range<span className="text-destructive">*</span>
              </Label>
              <div className="flex items-center gap-2">
                <div className="relative flex-1">
                  <Input
                    type="date"
                    value={startDate}
                    onChange={(e) => setStartDate(e.target.value)}
                    className="text-sm"
                  />
                </div>
                <span className="text-muted-foreground">-</span>
                <div className="relative flex-1">
                  <Input
                    type="date"
                    value={endDate}
                    onChange={(e) => setEndDate(e.target.value)}
                    className="text-sm"
                  />
                </div>
              </div>
              <p className="text-xs text-muted-foreground">Start time is 12 am US/Eastern</p>
            </div>
          </div>
        </div>

        <DialogFooter>
          <Button variant="outline" onClick={handleCancel}>
            Cancel
          </Button>
          <Button
            onClick={handleSubmit}
            disabled={!sessionName || !selectedUniverse || !startDate || !endDate}
          >
            Create session
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
