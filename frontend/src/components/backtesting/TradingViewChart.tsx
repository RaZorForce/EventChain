import { useEffect, useRef } from 'react';
import { createChart, ColorType, CandlestickSeries, HistogramSeries } from 'lightweight-charts';
import type { CandlestickData, Time, IChartApi, ISeriesApi, HistogramData } from 'lightweight-charts';

// Extended type that includes volume
export interface OHLCVData extends CandlestickData<Time> {
  volume?: number;
}

interface TradingViewChartProps {
  data: OHLCVData[];
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
  const volumeSeriesRef = useRef<ISeriesApi<'Histogram'> | null>(null);

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
          bottom: 0.2, // Leave room for volume at the bottom
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

    // Create volume histogram series
    const volumeSeries = chart.addSeries(HistogramSeries, {
      color: '#26a69a',
      priceFormat: {
        type: 'volume',
      },
      priceScaleId: 'volume', // Use separate price scale
    });

    // Configure volume price scale
    chart.priceScale('volume').applyOptions({
      scaleMargins: {
        top: 0.85, // Volume takes bottom 15% of the chart
        bottom: 0,
      },
    });

    volumeSeriesRef.current = volumeSeries;

    // Set data
    if (data.length > 0) {
      candlestickSeries.setData(data);

      // Set volume data with colors based on price direction
      const volumeData: HistogramData<Time>[] = data.map((candle) => {
        const isUp = candle.close >= candle.open;
        return {
          time: candle.time,
          value: candle.volume || 0,
          color: isUp ? 'rgba(38, 166, 154, 0.5)' : 'rgba(239, 83, 80, 0.5)',
        };
      });
      volumeSeries.setData(volumeData);

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

      // Also update volume data
      if (volumeSeriesRef.current) {
        const volumeData: HistogramData<Time>[] = data.map((candle) => {
          const isUp = candle.close >= candle.open;
          return {
            time: candle.time,
            value: candle.volume || 0,
            color: isUp ? 'rgba(38, 166, 154, 0.5)' : 'rgba(239, 83, 80, 0.5)',
          };
        });
        volumeSeriesRef.current.setData(volumeData);
      }

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

// Helper function to generate sample candlestick data with volume
export function generateSampleCandleData(
  startDate: Date,
  endDate: Date,
  timeframeMinutes: number = 1,
  startPrice: number = 100
): OHLCVData[] {
  const data: OHLCVData[] = [];
  let currentTime = new Date(startDate);
  let price = startPrice;

  // Base volume that varies throughout the day
  const baseVolume = 100000;

  while (currentTime <= endDate) {
    const open = price;
    const change = (Math.random() - 0.5) * 2;
    const volatility = Math.random() * 1.5;
    const high = Math.max(open, open + change) + volatility;
    const low = Math.min(open, open + change) - volatility;
    const close = open + change;
    price = close;

    // Generate volume with some randomness
    // Higher volume during market open/close hours (9-10am, 3-4pm)
    const hour = currentTime.getHours();
    let volumeMultiplier = 1;
    if (hour >= 9 && hour < 10) volumeMultiplier = 1.5;
    else if (hour >= 15 && hour < 16) volumeMultiplier = 1.3;
    else if (hour < 9 || hour >= 16) volumeMultiplier = 0.3; // After hours

    const volume = Math.round(
      baseVolume * volumeMultiplier * (0.5 + Math.random())
    );

    data.push({
      time: (currentTime.getTime() / 1000) as Time,
      open: parseFloat(open.toFixed(2)),
      high: parseFloat(high.toFixed(2)),
      low: parseFloat(low.toFixed(2)),
      close: parseFloat(close.toFixed(2)),
      volume,
    });

    currentTime = new Date(currentTime.getTime() + timeframeMinutes * 60 * 1000);
  }

  return data;
}
