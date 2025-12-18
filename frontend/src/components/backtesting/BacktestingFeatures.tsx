import { Card } from '@/components/ui/card';
import { Globe, Flame, Zap, MessageSquare } from 'lucide-react';
import type { LucideIcon } from 'lucide-react';

interface FeatureCard {
  icon: LucideIcon;
  title: string;
  description: string;
  comingSoon?: boolean;
}

const features: FeatureCard[] = [
  {
    icon: Globe,
    title: 'Travel Back In Time',
    description: 'Navigate through historical market conditions and set the pace at your own speed',
  },
  {
    icon: Flame,
    title: 'Simulate Trades',
    description: 'Witness your strategy come to life in real time by simulating trades and putting your strategy to the test',
  },
  {
    icon: Zap,
    title: 'Trading Journal & Analytics Unleashed',
    description: 'Have a detailed record of every trade, reflect on each trading session, and get access to a wealth of analytics',
  },
  {
    icon: MessageSquare,
    title: 'Share Your Sessions',
    description: 'Collaborate with others by sharing your sessions. Every trade taken, all the data, and the ability to replay the session yourself, all at your fingertips.',
    comingSoon: true,
  },
];

export function BacktestingFeatures() {
  return (
    <div className="py-12 px-6">
      {/* Section Header */}
      <div className="text-center mb-10">
        <h2 className="text-2xl font-semibold text-foreground italic mb-3">
          The Ultimate Tool to Test Your Strategies
        </h2>
        <p className="text-muted-foreground max-w-xl mx-auto">
          Put your strategy to the test by simulating trades on all asset types and receive analytics
        </p>
      </div>

      {/* Features Grid */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-6 max-w-4xl mx-auto">
        {features.map((feature) => (
          <Card
            key={feature.title}
            className="p-6 bg-card border-border hover:border-primary/30 transition-colors"
          >
            <div className="flex gap-4">
              {/* Icon */}
              <div className="shrink-0">
                <div className="w-10 h-10 rounded-lg bg-amber-500/10 flex items-center justify-center">
                  <feature.icon className="w-5 h-5 text-amber-500" />
                </div>
              </div>

              {/* Content */}
              <div className="flex-1">
                <div className="flex items-center gap-2 mb-2">
                  <h3 className="font-semibold text-foreground">
                    {feature.title}
                  </h3>
                  {feature.comingSoon && (
                    <span className="text-[10px] uppercase tracking-wider text-muted-foreground bg-muted px-2 py-0.5 rounded">
                      Coming Soon
                    </span>
                  )}
                </div>
                <p className="text-sm text-muted-foreground leading-relaxed">
                  {feature.description}
                </p>
              </div>
            </div>
          </Card>
        ))}
      </div>
    </div>
  );
}
