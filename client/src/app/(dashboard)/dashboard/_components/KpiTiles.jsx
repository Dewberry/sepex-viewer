"use client";

import { CheckCircle2, Loader2, TrendingUp, XCircle } from "lucide-react";
import { Skeleton } from "@/components/ui/skeleton";

function Tile({ label, value, icon, accentClass }) {
  return (
    <div className="rounded-lg border border-border bg-card p-4 transition-shadow hover:shadow-md">
      <div className="mb-1 flex items-center gap-1.5 text-xs text-muted-foreground">
        {icon ? <span className={accentClass}>{icon}</span> : null}
        {label}
      </div>
      <div className="text-2xl font-bold">{value}</div>
    </div>
  );
}

export default function KpiTiles({ kpis, isLoading }) {
  if (isLoading) {
    return (
      <div className="grid grid-cols-2 gap-4 md:grid-cols-5">
        {Array.from({ length: 5 }).map((_, i) => (
          <Skeleton key={i} className="h-[88px] rounded-lg" />
        ))}
      </div>
    );
  }

  const successRate = kpis.successRate === null ? "—" : `${kpis.successRate}%`;

  return (
    <div className="grid grid-cols-2 gap-4 md:grid-cols-5">
      <Tile label="Total Jobs" value={kpis.total} />
      <Tile
        label="Successful"
        value={kpis.successful}
        icon={<CheckCircle2 className="h-3.5 w-3.5" />}
        accentClass="text-status-successful"
      />
      <Tile
        label="Failed"
        value={kpis.failed}
        icon={<XCircle className="h-3.5 w-3.5" />}
        accentClass="text-status-failed"
      />
      <Tile
        label="Running"
        value={kpis.running}
        icon={
          <Loader2
            className={`h-3.5 w-3.5 ${kpis.running > 0 ? "animate-spin" : ""}`}
          />
        }
        accentClass="text-status-running"
      />
      <Tile
        label="Success Rate"
        value={successRate}
        icon={<TrendingUp className="h-3.5 w-3.5" />}
      />
    </div>
  );
}
