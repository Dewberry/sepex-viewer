"use client";

import {
  Bar,
  BarChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from "recharts";

function ChartTooltip({ active, payload }) {
  if (!active || !payload || payload.length === 0) return null;
  const entry = payload[0];
  return (
    <div className="rounded-md border border-border bg-popover px-3 py-2 text-xs shadow-md">
      <div className="font-mono text-popover-foreground">
        {entry.payload.submitter}
      </div>
      <div className="text-muted-foreground">{entry.value} jobs</div>
    </div>
  );
}

export default function TopSubmittersChart({ data }) {
  const height = Math.max(160, data.length * 32 + 32);
  return (
    <div className="rounded-lg border border-border bg-card p-4">
      <h3 className="mb-4 font-semibold">Top Submitters</h3>
      <div className="w-full" style={{ height }}>
        {data.length === 0 ? (
          <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
            No jobs in this window.
          </div>
        ) : (
          <ResponsiveContainer width="100%" height="100%">
            <BarChart
              data={data}
              layout="vertical"
              margin={{ top: 4, right: 16, left: 0, bottom: 0 }}
            >
              <CartesianGrid
                strokeDasharray="3 3"
                stroke="var(--border)"
                horizontal={false}
              />
              <XAxis
                type="number"
                tickLine={false}
                axisLine={false}
                allowDecimals={false}
                tick={{ fill: "var(--muted-foreground)", fontSize: 11 }}
              />
              <YAxis
                type="category"
                dataKey="submitter"
                tickLine={false}
                axisLine={false}
                width={100}
                tick={{ fill: "var(--muted-foreground)", fontSize: 11 }}
              />
              <Tooltip
                cursor={{ fill: "var(--muted)", opacity: 0.4 }}
                content={<ChartTooltip />}
              />
              <Bar
                dataKey="count"
                fill="var(--dewberry-teal)"
                radius={[0, 4, 4, 0]}
              />
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  );
}
