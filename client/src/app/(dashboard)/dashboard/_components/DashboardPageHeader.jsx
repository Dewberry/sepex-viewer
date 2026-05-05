"use client";

import { RefreshCw } from "lucide-react";
import TimeRangePicker from "@/app/(dashboard)/dashboard/_components/TimeRangePicker";
import { Button } from "@/components/ui/button";

export default function DashboardPageHeader({
  range,
  onRangeChange,
  isFetching,
  onRefresh
}) {
  return (
    <div className="flex flex-wrap items-start justify-between gap-4">
      <div>
        <h1 className="text-3xl font-bold">Dashboard</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          System health and recent activity at a glance.
        </p>
      </div>
      <div className="flex items-center gap-3">
        <div className="flex flex-col items-end gap-1">
          <TimeRangePicker value={range} onChange={onRangeChange} />
          {range !== "24h" ? (
            <span className="text-[11px] text-muted-foreground">
              Best-effort — backend filtering pending
            </span>
          ) : null}
        </div>
        <Button
          variant="ghost"
          size="icon"
          onClick={onRefresh}
          aria-label="Refresh dashboard"
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
