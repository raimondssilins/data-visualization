import React, { useEffect, useRef } from "react";
import Chart from "chart.js/auto";

Chart.defaults.backgroundColor = "white";
Chart.defaults.borderColor = "gray";
Chart.defaults.color = "#000";

interface ChartCustomProps {
  data: Record<string, number>;
  chartLabel: string;
  isUpdated?: boolean;
}
const ChartCustom: React.FC<ChartCustomProps> = ({
  data,
  chartLabel,
  isUpdated,
}) => {
  const chartRef = useRef<HTMLCanvasElement | null>(null);
  const chartInstance = useRef<Chart | null>(null);

  useEffect(() => {
    if (!chartRef.current) return;

    const ctx = chartRef.current.getContext("2d", {});

    if (!ctx) {
      console.error("Canvas context is null");
      return;
    }

    // If there is an existing chart instance, destroy it
    if (chartInstance.current) {
      chartInstance.current.destroy();
      chartInstance.current = null;
    }

    // Create a new chart instance
    chartInstance.current = new Chart(ctx, {
      type: "bar",
      data: {
        labels: [chartLabel],
        datasets: [
          {
            label: chartLabel,
            data: [],
            backgroundColor: "rgba(141, 4, 4, 0.8)",
            borderColor: "rgba(3, 3, 3, 1)",
            borderWidth: 0.1,
          },
        ],
      },
      options: {
        onClick: (e) => {
          console.log(e);
        },
        layout: {
          padding: 20,
        },
        scales: {
          x: {
            type: "category",
            labels: Object.keys(data),
          },
          y: {
            beginAtZero: true,
            title: {
              display: true,
            },
          },
        },
      },
    });

    console.log(data);

    // Update the chart data
    if (chartInstance.current) {
      const labels = Object.keys(data);
      const values = Object.values(data);

      chartInstance.current.data.labels = labels;
      chartInstance.current.data.datasets[0].data = values;

      // Update the chart
      chartInstance.current.update();
    }
  }, [data, chartLabel, isUpdated, chartRef]);

  return <canvas ref={chartRef} />;
};

export default ChartCustom;
