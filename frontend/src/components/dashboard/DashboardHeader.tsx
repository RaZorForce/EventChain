import { Button } from '@/components/ui/button';
import {
  ChevronDown,
  Filter,
  Calendar,
  Settings,
  Play,
  DollarSign,
  RefreshCw,
} from 'lucide-react';

interface DashboardHeaderProps {
  lastImport: string;
  onResync?: () => void;
}

function formatDate(dateStr: string): string {
  const date = new Date(dateStr);
  return date.toLocaleDateString('en-US', {
    month: 'short',
    day: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

export function DashboardHeader({ lastImport, onResync }: DashboardHeaderProps) {
  return (
    <div className="flex items-center justify-between py-4 px-6 border-b bg-background">
      {/* Left Section */}
      <div className="flex items-center gap-4">
        <h1 className="text-xl font-semibold">Dashboard</h1>
        <div className="flex items-center gap-2 text-sm text-muted-foreground">
          <span>Last Import: {formatDate(lastImport)}</span>
          <button
            onClick={onResync}
            className="text-primary hover:underline flex items-center gap-1"
          >
            <RefreshCw className="h-3 w-3" />
            Resync
          </button>
        </div>
      </div>

      {/* Right Section */}
      <div className="flex items-center gap-3">
        {/* Currency Selector */}
        <Button variant="outline" size="sm" className="gap-1">
          <DollarSign className="h-4 w-4" />
          <ChevronDown className="h-3 w-3" />
        </Button>

        {/* Filters Button */}
        <Button variant="outline" size="sm" className="gap-2">
          <Filter className="h-4 w-4" />
          Filters
          <ChevronDown className="h-3 w-3" />
        </Button>

        {/* Date Range Picker */}
        <Button variant="outline" size="sm" className="gap-2">
          <Calendar className="h-4 w-4" />
          Feb 01, 2025 - Feb 28, 2025
          <ChevronDown className="h-3 w-3" />
        </Button>

        {/* Account Selector */}
        <Button variant="outline" size="sm" className="gap-2">
          All accounts
          <ChevronDown className="h-3 w-3" />
        </Button>

        {/* Start My Day Button */}
        <Button size="sm" className="gap-2 bg-teal-500 hover:bg-teal-600 text-white">
          <Play className="h-4 w-4" />
          Start my day
        </Button>

        {/* Settings */}
        <Button variant="ghost" size="icon">
          <Settings className="h-5 w-5" />
        </Button>
      </div>
    </div>
  );
}
