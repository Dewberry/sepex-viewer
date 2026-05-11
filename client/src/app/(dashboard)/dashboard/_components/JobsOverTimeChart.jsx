"use client";

import {
  Area,
  AreaChart,
  CartesianGrid,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from "recharts";
import { Skeleton } from "@/components/ui/skeleton";

const STACKS = [
  { key: "successful", color: "var(--status-successful)", label: "Successful" },
  { key: "failed", color: "var(--status-failed)", label: "Failed" },
  { key: "running", color: "var(--status-running)", label: "Running" }
];

function ChartTooltip({ active, payload, label }) {
  if (!active || !payload || payload.length === 0) return null;
  const total = payload.reduce((sum, p) => sum + (p.value || 0), 0);
  return (
    <div className="rounded-md border border-border bg-popover px-3 py-2 text-xs shadow-md">
      <div className="mb-1 font-mono text-popover-foreground">{label}</div>
      {payload
        .slice()
        .reverse()
        .map((p) => (
          <div key={p.dataKey} className="flex items-center gap-2">
            <span
              className="h-2 w-2 rounded-sm"
              style={{ backgroundColor: p.color }}
            />
            <span className="capitalize text-muted-foreground">
              {p.dataKey}
            </span>
            <span className="ml-auto font-mono">{p.value}</span>
          </div>
        ))}
      <div className="mt-1 flex items-center gap-2 border-t border-border pt-1 text-muted-foreground">
        <span>Total</span>
        <span className="ml-auto font-mono">{total}</span>
      </div>
    </div>
  );
}

function buildChartSummary(data) {
  if (!data || data.length === 0) return "No data.";
  const totals = STACKS.reduce((acc, s) => {
    acc[s.key] = data.reduce((sum, d) => sum + (d[s.key] || 0), 0);
    return acc;
  }, {});
  const grand = STACKS.reduce((sum, s) => sum + totals[s.key], 0);
  const parts = STACKS.map((s) => `${totals[s.key]} ${s.label.toLowerCase()}`);
  return `${grand} jobs over ${data.length} buckets — ${parts.join(", ")}.`;
}

export default function JobsOverTimeChart({ data, isLoading, isError }) {
  return (
    <section
      className="rounded-lg border border-border bg-card p-4"
      aria-labelledby="jobs-over-time-heading"
    >
      <h2 id="jobs-over-time-heading" className="mb-4 font-semibold">
        Jobs Over Time
      </h2>
      {isLoading ? (
        <Skeleton className="h-64 w-full rounded-md" />
      ) : isError ? (
        <div className="flex h-64 items-center justify-center text-sm text-muted-foreground">
          Couldn&rsquo;t load jobs.
        </div>
      ) : !data || data.length === 0 ? (
        <div className="flex h-64 items-center justify-center text-sm text-muted-foreground">
          No jobs in this window.
        </div>
      ) : (
        <>
          <div
            role="img"
            aria-label={`Jobs over time stacked area chart. ${buildChartSummary(data)}`}
            className="h-64 w-full"
          >
            <ResponsiveContainer width="100%" height="100%">
              <AreaChart
                data={data}
                margin={{ top: 4, right: 8, left: 0, bottom: 0 }}
              >
                <defs>
                  {STACKS.map((s) => (
                    <linearGradient
                      key={s.key}
                      id={`fill-${s.key}`}
                      x1="0"
                      y1="0"
                      x2="0"
                      y2="1"
                    >
                      <stop offset="0%" stopColor={s.color} stopOpacity={0.9} />
                      <stop
                        offset="100%"
                        stopColor={s.color}
                        stopOpacity={0.45}
                      />
                    </linearGradient>
                  ))}
                </defs>
                <CartesianGrid
                  strokeDasharray="3 3"
                  stroke="var(--border)"
                  vertical={false}
                />
                <XAxis
                  dataKey="label"
                  tickLine={false}
                  axisLine={false}
                  interval="preserveStartEnd"
                  minTickGap={24}
                  tick={{ fill: "var(--muted-foreground)", fontSize: 11 }}
                />
                <YAxis
                  allowDecimals={false}
                  tickLine={false}
                  axisLine={false}
                  width={28}
                  tick={{ fill: "var(--muted-foreground)", fontSize: 11 }}
                />
                <Tooltip
                  cursor={{ stroke: "var(--border)", strokeWidth: 1 }}
                  content={<ChartTooltip />}
                />
                {STACKS.map((s) => (
                  <Area
                    key={s.key}
                    type="monotone"
                    dataKey={s.key}
                    stackId="1"
                    stroke={s.color}
                    strokeWidth={1.5}
                    fill={`url(#fill-${s.key})`}
                    isAnimationActive={false}
                  />
                ))}
              </AreaChart>
            </ResponsiveContainer>
          </div>
          <div className="mt-3 flex flex-wrap items-center gap-3 text-xs">
            {STACKS.map((s) => (
              <div key={s.key} className="flex items-center gap-1.5">
                <span
                  className="h-3 w-3 rounded-sm"
                  style={{ backgroundColor: s.color }}
                />
                <span className="text-muted-foreground">{s.label}</span>
              </div>
            ))}
          </div>
        </>
      )}
    </section>
  );
}
