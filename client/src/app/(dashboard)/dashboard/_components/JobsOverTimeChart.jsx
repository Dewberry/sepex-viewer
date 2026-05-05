"use client";

import {
  Bar,
  BarChart,
  CartesianGrid,
  Cell,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis
} from "recharts";

const STACKS = [
  { key: "successful", color: "var(--status-successful)", label: "Successful" },
  { key: "failed", color: "var(--status-failed)", label: "Failed" },
  { key: "running", color: "var(--status-running)", label: "Running" },
  { key: "accepted", color: "var(--status-accepted)", label: "Accepted" }
];

function ChartTooltip({ active, payload, label }) {
  if (!active || !payload || payload.length === 0) return null;
  const inProgress = payload[0]?.payload?.inProgress;
  return (
    <div className="rounded-md border border-border bg-popover px-3 py-2 text-xs shadow-md">
      <div className="mb-1 flex items-center gap-2 font-mono text-popover-foreground">
        <span>{label}</span>
        {inProgress ? (
          <span className="text-[10px] text-muted-foreground">
            (in progress)
          </span>
        ) : null}
      </div>
      {payload
        .filter((p) => p.value > 0)
        .map((p) => (
          <div key={p.dataKey} className="flex items-center gap-2">
            <span
              className="h-2 w-2 rounded-sm"
              style={{ backgroundColor: p.color }}
            />
            <span className="text-muted-foreground capitalize">{p.name}</span>
            <span className="ml-auto font-medium text-popover-foreground">
              {p.value}
            </span>
          </div>
        ))}
    </div>
  );
}

export default function JobsOverTimeChart({ data }) {
  return (
    <div className="rounded-lg border border-border bg-card p-4">
      <h3 className="mb-4 font-semibold">Jobs Over Time</h3>
      <div className="h-64 w-full">
        {data.length === 0 ? (
          <div className="flex h-full items-center justify-center text-sm text-muted-foreground">
            No jobs in this window.
          </div>
        ) : (
          <ResponsiveContainer width="100%" height="100%">
            <BarChart
              data={data}
              margin={{ top: 8, right: 8, left: 0, bottom: 0 }}
            >
              <CartesianGrid
                strokeDasharray="3 3"
                stroke="var(--border)"
                vertical={false}
              />
              <XAxis
                dataKey="label"
                tickLine={false}
                axisLine={false}
                tick={{ fill: "var(--muted-foreground)", fontSize: 11 }}
              />
              <YAxis
                tickLine={false}
                axisLine={false}
                allowDecimals={false}
                tick={{ fill: "var(--muted-foreground)", fontSize: 11 }}
                width={28}
              />
              <Tooltip
                cursor={{ fill: "var(--muted)", opacity: 0.4 }}
                content={<ChartTooltip />}
              />
              <Legend
                iconSize={10}
                wrapperStyle={{
                  fontSize: 11,
                  color: "var(--muted-foreground)"
                }}
              />
              {STACKS.map((s) => (
                <Bar
                  key={s.key}
                  dataKey={s.key}
                  name={s.label}
                  stackId="status"
                  fill={s.color}
                >
                  {data.map((entry) => (
                    <Cell
                      key={entry.key}
                      fillOpacity={entry.inProgress ? 0.5 : 1}
                    />
                  ))}
                </Bar>
              ))}
            </BarChart>
          </ResponsiveContainer>
        )}
      </div>
    </div>
  );
}
