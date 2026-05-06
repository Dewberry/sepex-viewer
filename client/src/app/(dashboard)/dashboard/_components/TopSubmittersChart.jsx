"use client";

import { Skeleton } from "@/components/ui/skeleton";

export default function TopSubmittersChart({ data, isLoading, isError }) {
  const total = data?.reduce((sum, e) => sum + e.count, 0) || 0;

  return (
    <div className="rounded-lg border border-border bg-card p-4">
      <h3 className="mb-4 font-semibold">Top Submitters</h3>
      {isLoading ? (
        <Skeleton className="h-64 w-full rounded-md" />
      ) : isError ? (
        <div className="flex h-32 items-center justify-center text-sm text-muted-foreground">
          Couldn&rsquo;t load jobs.
        </div>
      ) : !data || data.length === 0 ? (
        <div className="flex h-32 items-center justify-center text-sm text-muted-foreground">
          No jobs in this window.
        </div>
      ) : (
        <div className="space-y-3">
          {data.map((entry) => {
            const pct = total > 0 ? (entry.count / total) * 100 : 0;
            return (
              <div key={entry.submitter}>
                <div className="mb-1 flex justify-between text-xs">
                  <span
                    className="truncate font-mono text-muted-foreground"
                    title={entry.submitter}
                  >
                    {entry.submitter}
                  </span>
                  <span className="font-semibold">{entry.count}</span>
                </div>
                <div className="h-2 overflow-hidden rounded-full bg-muted">
                  <div
                    className="h-full rounded-full bg-status-successful transition-all"
                    style={{ width: `${pct}%` }}
                  />
                </div>
              </div>
            );
          })}
        </div>
      )}
    </div>
  );
}
