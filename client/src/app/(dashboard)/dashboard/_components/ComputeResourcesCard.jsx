"use client";

import { useEffect, useState } from "react";
import { Cpu, MemoryStick } from "lucide-react";
import { getLastUpdatedLabel } from "@/app/(dashboard)/dashboard/_utils/relativeTime";
import { Skeleton } from "@/components/ui/skeleton";

function Bar({ usedPct = 0, queuedPct = 0, accentVar }) {
  const used = Math.min(100, Math.max(0, usedPct));
  const queued = Math.min(100 - used, Math.max(0, queuedPct));
  return (
    <div className="flex h-3 overflow-hidden rounded-full bg-muted">
      <div
        className="h-full transition-all"
        style={{ width: `${used}%`, backgroundColor: `var(${accentVar})` }}
      />
      <div
        className="h-full transition-all"
        style={{
          width: `${queued}%`,
          backgroundColor: `color-mix(in srgb, var(${accentVar}) 30%, transparent)`
        }}
      />
    </div>
  );
}

function Gauge({
  icon,
  label,
  used,
  max,
  unit,
  usedPct,
  queuedPct,
  accentVar
}) {
  return (
    <div>
      <div className="mb-2 flex items-center justify-between">
        <div className="flex items-center gap-2 text-sm">
          <span style={{ color: `var(${accentVar})` }}>{icon}</span>
          <span>{label}</span>
        </div>
        <div className="font-mono text-sm">
          <span className="font-semibold">
            {used}
            {unit}
          </span>
          <span className="text-muted-foreground">
            /{max}
            {unit}
          </span>
        </div>
      </div>
      <Bar usedPct={usedPct} queuedPct={queuedPct} accentVar={accentVar} />
      <div className="mt-1 flex justify-between text-xs text-muted-foreground">
        <span>{usedPct ?? 0}% used</span>
        <span>{queuedPct ?? 0}% queued</span>
      </div>
    </div>
  );
}

export default function ComputeResourcesCard({
  data,
  isLoading,
  isError,
  dataUpdatedAt
}) {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, []);

  return (
    <div className="rounded-lg border border-border bg-card p-4">
      <div className="mb-4 flex items-center justify-between">
        <h3 className="font-semibold">Compute Resources</h3>
        <span className="text-[11px] text-muted-foreground">
          Updated {getLastUpdatedLabel(now, dataUpdatedAt)}
        </span>
      </div>

      {isLoading ? (
        <div className="space-y-4">
          <Skeleton className="h-12 w-full" />
          <Skeleton className="h-12 w-full" />
        </div>
      ) : isError ? (
        <div className="text-sm text-muted-foreground">
          Couldn&rsquo;t reach{" "}
          <span className="font-mono">/admin/resources</span>.
        </div>
      ) : (
        <div className="space-y-4">
          <Gauge
            icon={<Cpu className="h-4 w-4" />}
            label="CPU"
            used={data?.usedCPUs ?? 0}
            max={data?.maxCPUs ?? 0}
            unit=""
            usedPct={data?.usedCPUsPct}
            queuedPct={data?.queuedCPUsPct}
            accentVar="--chart-1"
          />
          <Gauge
            icon={<MemoryStick className="h-4 w-4" />}
            label="Memory"
            used={data?.usedMemory ?? 0}
            max={data?.maxMemory ?? 0}
            unit="GB"
            usedPct={data?.usedMemPct}
            queuedPct={data?.queuedMemPct}
            accentVar="--chart-4"
          />
        </div>
      )}

      <p className="mt-3 text-xs text-muted-foreground">
        * Local Docker / subprocess pool only — AWS Batch jobs not included
      </p>
    </div>
  );
}
