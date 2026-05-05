"use client";

import { useEffect, useState } from "react";
import { Copy, Server } from "lucide-react";
import { toast } from "sonner";
import DismissJobButton from "@/app/(dashboard)/jobs/[jobID]/_components/DismissJobButton";
import StatusIcon from "@/app/(dashboard)/jobs/[jobID]/_components/StatusIcon";
import { formatElapsed } from "@/app/(dashboard)/jobs/[jobID]/_utils/formatElapsed";
import { getRelativeTime } from "@/app/(dashboard)/jobs/_utils/relativeTime";
import { ACTIVE_STATUSES } from "@/lib/sepex";

export default function RunSummaryCard({ job }) {
  const isActive = ACTIVE_STATUSES.has(job.status);

  // While the job is active, advance the elapsed clock once a second so users
  // see the timer move even between the 2s status polls.
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (!isActive) return undefined;
    const id = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(id);
  }, [isActive]);

  const elapsedEnd = isActive ? new Date(now).toISOString() : job.updated;

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(job.jobID);
      toast.success("Copied");
    } catch {
      toast.error("Couldn’t copy");
    }
  };

  return (
    <div className="rounded-lg border border-border bg-card p-4">
      <div className="flex flex-col gap-4 lg:flex-row lg:items-center">
        <div className="flex min-w-0 flex-1 items-start gap-3">
          <StatusIcon status={job.status} className="h-5 w-5 shrink-0" />
          <div className="min-w-0 flex-1 space-y-1">
            <button
              type="button"
              onClick={handleCopy}
              title="Copy job ID"
              className="group inline-flex max-w-full items-center gap-2 font-mono text-sm font-medium hover:text-dewberry-teal"
            >
              <span className="truncate">{job.jobID}</span>
              <Copy className="h-3 w-3 shrink-0 opacity-60 group-hover:opacity-100" />
            </button>
            <div className="flex flex-wrap items-center gap-x-3 gap-y-1 text-sm text-muted-foreground">
              <span className="font-medium text-foreground/80">
                {job.processID}
              </span>
              {job.submitter ? (
                <>
                  <span>·</span>
                  <span>{job.submitter}</span>
                </>
              ) : null}
              {job.host ? (
                <>
                  <span>·</span>
                  <span className="inline-flex items-center gap-1">
                    <Server className="h-3 w-3" />
                    {job.host}
                  </span>
                </>
              ) : null}
              {job.mode ? (
                <>
                  <span>·</span>
                  <span>{job.mode}</span>
                </>
              ) : null}
            </div>
            {job.hostJobID ? (
              <div className="font-mono text-xs break-all text-muted-foreground">
                {job.host ? `${job.host}://` : ""}
                {job.hostJobID}
              </div>
            ) : null}
          </div>
        </div>

        <div className="flex gap-6 text-sm">
          <div>
            <div className="text-xs tracking-wider text-muted-foreground uppercase">
              Elapsed
            </div>
            <div className="font-mono">
              {formatElapsed(job.created, elapsedEnd)}
            </div>
          </div>
          <div>
            <div className="text-xs tracking-wider text-muted-foreground uppercase">
              Updated
            </div>
            <div className="font-mono">{getRelativeTime(job.updated)}</div>
          </div>
        </div>

        {isActive ? (
          <div className="flex gap-2">
            <DismissJobButton jobID={job.jobID} />
          </div>
        ) : null}
      </div>
    </div>
  );
}
