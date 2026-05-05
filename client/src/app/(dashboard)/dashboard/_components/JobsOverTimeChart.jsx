"use client";

import { Skeleton } from "@/components/ui/skeleton";

const STACKS = [
  { key: "successful", color: "var(--status-successful)", label: "Successful" },
  { key: "failed", color: "var(--status-failed)", label: "Failed" },
  { key: "running", color: "var(--status-running)", label: "Running" },
  { key: "accepted", color: "var(--status-accepted)", label: "Accepted" }
];

export default function JobsOverTimeChart({ data, isLoading, isError }) {
  return (
    <div className="rounded-lg border border-border bg-card p-4">
      <h3 className="mb-4 font-semibold">Jobs Over Time</h3>
      {isLoading ? (
        <Skeleton className="h-64 w-full rounded-md" />
      ) : isError ? (
        <div className="flex h-32 items-center justify-center text-sm text-muted-foreground">
          Couldn&rsquo;t load jobs.
        </div>
      ) : data.length === 0 ? (
        <div className="flex h-32 items-center justify-center text-sm text-muted-foreground">
          No jobs in this window.
        </div>
      ) : (
        <div className="space-y-3">
          {data.map((bucket) => {
            const total = STACKS.reduce(
              (sum, s) => sum + (bucket[s.key] || 0),
              0
            );
            return (
              <div key={bucket.key} className="flex items-center gap-3">
                <div className="w-12 font-mono text-xs text-muted-foreground">
                  {bucket.label}
                </div>
                <div
                  className={`flex h-6 flex-1 overflow-hidden rounded ${
                    bucket.inProgress ? "opacity-60" : ""
                  } ${total === 0 ? "bg-muted" : ""}`}
                >
                  {STACKS.map((s) => {
                    const value = bucket[s.key] || 0;
                    if (value === 0 || total === 0) return null;
                    return (
                      <div
                        key={s.key}
                        className="transition-opacity hover:opacity-80"
                        style={{
                          width: `${(value / total) * 100}%`,
                          backgroundColor: s.color
                        }}
                        title={`${s.label}: ${value}`}
                      />
                    );
                  })}
                </div>
                <div className="w-8 text-right text-xs text-muted-foreground">
                  {total}
                </div>
              </div>
            );
          })}
          <div className="flex flex-wrap items-center gap-3 pt-2 text-xs">
            {STACKS.map((s) => (
              <div key={s.key} className="flex items-center gap-1.5">
                <div
                  className="h-3 w-3 rounded-sm"
                  style={{ backgroundColor: s.color }}
                />
                <span className="text-muted-foreground">{s.label}</span>
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
