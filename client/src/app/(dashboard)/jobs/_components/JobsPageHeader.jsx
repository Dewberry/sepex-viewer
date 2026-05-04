"use client";

import { useEffect, useState } from "react";
import { RefreshCw } from "lucide-react";
import { Button } from "@/components/ui/button";

function formatLastUpdated(now, dataUpdatedAt) {
  if (!dataUpdatedAt) return "never";
  const sec = Math.max(0, Math.floor((now - dataUpdatedAt) / 1000));
  if (sec < 5) return "just now";
  if (sec < 60) return `${sec}s ago`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m ago`;
  return `${Math.floor(min / 60)}h ago`;
}

export default function JobsPageHeader({
  resultCount,
  hasActiveJobs,
  dataUpdatedAt,
  isFetching,
  onRefresh
}) {
  const [now, setNow] = useState(() => Date.now());

  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, []);

  const lastUpdatedLabel = formatLastUpdated(now, dataUpdatedAt);

  return (
    <div className="flex flex-wrap items-start justify-between gap-4">
      <div>
        <h1 className="text-3xl font-bold">Jobs</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          {resultCount} {resultCount === 1 ? "result" : "results"}
        </p>
      </div>
      <div className="flex items-center gap-2">
        <div className="text-right">
          <div className="text-xs text-muted-foreground">
            Last updated {lastUpdatedLabel}
          </div>
          <div className="text-[11px] text-muted-foreground">
            {hasActiveJobs
              ? "Auto-refresh: every 15s when jobs are running"
              : "Auto-refresh: paused (no active jobs)"}
          </div>
        </div>
        <Button
          variant="ghost"
          size="icon"
          onClick={onRefresh}
          aria-label="Refresh"
          className="h-8 w-8"
        >
          <RefreshCw
            className={`h-4 w-4 ${isFetching ? "animate-spin" : ""}`}
          />
        </Button>
      </div>
    </div>
  );
}
