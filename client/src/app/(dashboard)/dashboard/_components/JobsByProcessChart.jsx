"use client";

import {
  Cell,
  Legend,
  Pie,
  PieChart,
  ResponsiveContainer,
  Tooltip
} from "recharts";

const PALETTE = [
  "var(--chart-1)",
  "var(--chart-2)",
  "var(--chart-3)",
  "var(--chart-4)",
  "var(--chart-5)",
  "var(--dewberry-teal)",
  "var(--dewberry-magenta)"
];

function ChartTooltip({ active, payload }) {
  if (!active || !payload || payload.length === 0) return null;
  const entry = payload[0];
  return (
    <div className="rounded-md border border-border bg-popover px-3 py-2 text-xs shadow-md">
      <div className="font-mono text-popover-foreground">
        {entry.payload.processID}
      </div>
      <div className="text-muted-foreground">{entry.value} jobs</div>
    </div>
  );
}

export default function JobsByProcessChart({ data }) {
  return (
    <div className="rounded-lg border border-border bg-card p-4">
      <h3 className="mb-4 font-semibold">Jobs by Process</h3>
      <div className="h-64 w-full">
        {data.length === 0 ? (
          <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
            No jobs in this window.
          </div>
        ) : (
          <ResponsiveContainer width="100%" height="100%">
            <PieChart>
              <Pie
                data={data}
                dataKey="count"
                nameKey="processID"
                cx="50%"
                cy="50%"
                innerRadius={50}
                outerRadius={85}
                paddingAngle={2}
                stroke="var(--card)"
              >
                {data.map((entry, index) => (
                  <Cell
                    key={entry.processID}
                    fill={PALETTE[index % PALETTE.length]}
                  />
                ))}
              </Pie>
              <Tooltip content={<ChartTooltip />} />
              <Legend
                iconSize={10}
                wrapperStyle={{
                  fontSize: 11,
                  color: "var(--muted-foreground)"
                }}
              />
            </PieChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  );
}
