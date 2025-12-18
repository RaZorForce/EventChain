import { useEffect, useRef } from 'react';
import { createChart, ColorType, CandlestickSeries } from 'lightweight-charts';
import type { CandlestickData, Time, IChartApi, ISeriesApi } from 'lightweight-charts';

interface TradingViewChartProps {
  data: CandlestickData<Time>[];
  width?: number;
  height?: number;
  onCrosshairMove?: (time: Time | null, price: number | null) => void;
}

export function TradingViewChart({
  data,
  width,
  height,
  onCrosshairMove
}: TradingViewChartProps) {
  const chartContainerRef = useRef<HTMLDivElement>(null);
  const chartRef = useRef<IChartApi | null>(null);
  const candlestickSeriesRef = useRef<ISeriesApi<'Candlestick'> | null>(null);

  useEffect(() => {
    if (!chartContainerRef.current) return;

    // Create chart
    const chart = createChart(chartContainerRef.current, {
      layout: {
        background: { type: ColorType.Solid, color: '#131722' },
        textColor: '#d1d4dc',
      },
      grid: {
        vertLines: { color: '#1e222d' },
        horzLines: { color: '#1e222d' },
      },
      crosshair: {
        mode: 1, // Normal crosshair
        vertLine: {
          width: 1,
          color: '#758696',
          style: 3,
          labelBackgroundColor: '#2962FF',
        },
        horzLine: {
          width: 1,
          color: '#758696',
          style: 3,
          labelBackgroundColor: '#2962FF',
        },
      },
      rightPriceScale: {
        borderColor: '#2B2B43',
        scaleMargins: {
          top: 0.1,
          bottom: 0.1,
        },
      },
      timeScale: {
        borderColor: '#2B2B43',
        timeVisible: true,
        secondsVisible: false,
      },
      width: width || chartContainerRef.current.clientWidth,
      height: height || 500,
    });

    chartRef.current = chart;

    // Create candlestick series using new API
    const candlestickSeries = chart.addSeries(CandlestickSeries, {
      upColor: '#26a69a',
      downColor: '#ef5350',
      borderDownColor: '#ef5350',
      borderUpColor: '#26a69a',
      wickDownColor: '#ef5350',
      wickUpColor: '#26a69a',
    });

    candlestickSeriesRef.current = candlestickSeries;

    // Set data
    if (data.length > 0) {
      candlestickSeries.setData(data);
      chart.timeScale().fitContent();
    }

    // Handle crosshair move
    if (onCrosshairMove) {
      chart.subscribeCrosshairMove((param) => {
        if (!param.time || !param.point) {
          onCrosshairMove(null, null);
          return;
        }
        const price = param.seriesData.get(candlestickSeries);
        if (price && 'close' in price) {
          onCrosshairMove(param.time, price.close);
        }
      });
    }

    // Handle resize
    const handleResize = () => {
      if (chartContainerRef.current) {
        chart.applyOptions({
          width: chartContainerRef.current.clientWidth
        });
      }
    };

    window.addEventListener('resize', handleResize);

    return () => {
      window.removeEventListener('resize', handleResize);
      chart.remove();
    };
  }, []);

  // Update data when it changes
  useEffect(() => {
    if (candlestickSeriesRef.current && data.length > 0) {
      candlestickSeriesRef.current.setData(data);
      chartRef.current?.timeScale().fitContent();
    }
  }, [data]);

  // Update size when dimensions change
  useEffect(() => {
    if (chartRef.current && chartContainerRef.current) {
      chartRef.current.applyOptions({
        width: width || chartContainerRef.current.clientWidth,
        height: height || 500,
      });
    }
  }, [width, height]);

  return (
    <div
      ref={chartContainerRef}
      className="w-full h-full"
    />
  );
}

// Helper function to generate sample candlestick data
export function generateSampleCandleData(
  startDate: Date,
  endDate: Date,
  timeframeMinutes: number = 1,
  startPrice: number = 100
): CandlestickData<Time>[] {
  const data: CandlestickData<Time>[] = [];
  let currentTime = new Date(startDate);
  let price = startPrice;

  while (currentTime <= endDate) {
    const open = price;
    const change = (Math.random() - 0.5) * 2;
    const volatility = Math.random() * 1.5;
    const high = Math.max(open, open + change) + volatility;
    const low = Math.min(open, open + change) - volatility;
    const close = open + change;
    price = close;

    data.push({
      time: (currentTime.getTime() / 1000) as Time,
      open: parseFloat(open.toFixed(2)),
      high: parseFloat(high.toFixed(2)),
      low: parseFloat(low.toFixed(2)),
      close: parseFloat(close.toFixed(2)),
    });

    currentTime = new Date(currentTime.getTime() + timeframeMinutes * 60 * 1000);
  }

  return data;
}
