interface ScoreProgressProps {
  score: number;
}

export function ScoreProgress({ score }: ScoreProgressProps) {
  // Score ranges: 0-20 (red), 20-40 (yellow), 40-60 (green), 60-100 (gray)
  const segments = [
    { start: 0, end: 20, color: 'bg-red-500' },
    { start: 20, end: 40, color: 'bg-yellow-500' },
    { start: 40, end: 60, color: 'bg-green-500' },
    { start: 60, end: 80, color: 'bg-gray-300' },
    { start: 80, end: 100, color: 'bg-gray-300' },
  ];

  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <span className="text-sm text-muted-foreground">Your Trading Score</span>
        <span className="text-2xl font-bold">{score.toFixed(2)}</span>
      </div>
      <div className="relative">
        {/* Progress bar background */}
        <div className="h-2 rounded-full flex overflow-hidden">
          {segments.map((segment, index) => {
            const width = segment.end - segment.start;
            const isActive = score >= segment.start;
            const fillWidth = isActive
              ? Math.min(100, ((Math.min(score, segment.end) - segment.start) / width) * 100)
              : 0;

            return (
              <div
                key={index}
                className="relative h-full"
                style={{ width: `${width}%` }}
              >
                {/* Background segment */}
                <div className={`absolute inset-0 ${segment.color} opacity-20`} />
                {/* Filled portion */}
                {fillWidth > 0 && (
                  <div
                    className={`absolute inset-y-0 left-0 ${segment.color}`}
                    style={{ width: `${fillWidth}%` }}
                  />
                )}
              </div>
            );
          })}
        </div>
        {/* Scale labels */}
        <div className="flex justify-between mt-1">
          {[0, 20, 40, 60, 80, 100].map((value) => (
            <span key={value} className="text-xs text-muted-foreground">
              {value}
            </span>
          ))}
        </div>
      </div>
    </div>
  );
}
