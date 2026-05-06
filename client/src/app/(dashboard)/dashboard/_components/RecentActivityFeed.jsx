"use client";

import Link from "next/link";
import {
  ArrowRight,
  Ban,
  CheckCircle2,
  CircleHelp,
  Clock,
  Loader2,
  XCircle
} from "lucide-react";
import StatusPill from "@/app/(dashboard)/jobs/_components/StatusPill";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";

const STATUS_ICON = {
  successful: { Icon: CheckCircle2, className: "text-status-successful" },
  failed: { Icon: XCircle, className: "text-status-failed" },
  running: { Icon: Loader2, className: "text-status-running animate-spin" },
  accepted: { Icon: Clock, className: "text-status-accepted" },
  dismissed: { Icon: Ban, className: "text-status-dismissed" },
  lost: { Icon: CircleHelp, className: "text-status-lost" }
};

function StatusIcon({ status }) {
  const entry = STATUS_ICON[status];
  if (!entry) return null;
  const { Icon, className } = entry;
  return <Icon className={`h-4 w-4 ${className}`} />;
}

function formatTime(updated) {
  if (!updated) return "—";
  const d = new Date(updated);
  return Number.isNaN(d.getTime()) ? "—" : d.toLocaleTimeString();
}

export default function RecentActivityFeed({
  jobs,
  isLoading,
  isError,
  limit = 8
}) {
  return (
    <div className="rounded-lg border border-border bg-card p-4">
      <h3 className="mb-4 font-semibold">Recent Activity</h3>

      {isLoading ? (
        <div className="space-y-2">
          {Array.from({ length: 4 }).map((_, i) => (
            <Skeleton key={i} className="h-10 w-full" />
          ))}
        </div>
      ) : isError ? (
        <div className="text-sm text-muted-foreground">
          Couldn&rsquo;t fetch recent jobs.
        </div>
      ) : !jobs || jobs.length === 0 ? (
        <div className="py-8 text-center text-sm text-muted-foreground">
          No recent activity.
        </div>
      ) : (
        <ul className="space-y-2">
          {jobs.slice(0, limit).map((job) => (
            <li key={job.jobID}>
              <Link
                href={`/jobs/${job.jobID}`}
                className="-mx-2 flex items-center gap-3 rounded border-b border-border px-2 py-2 transition-colors last:border-0 hover:bg-accent/50"
              >
                <span className="flex w-8 items-center justify-center">
                  <StatusIcon status={job.status} />
                </span>
                <div className="min-w-0 flex-1">
                  <div className="flex items-center gap-2">
                    <span
                      className="truncate font-mono text-sm font-medium"
                      title={job.jobID}
                    >
                      {job.jobID}
                    </span>
                    <StatusPill status={job.status} />
                  </div>
                  <div className="truncate text-xs text-muted-foreground">
                    {job.processID || "—"}
                    {job.submitter ? <> · {job.submitter}</> : null}
                  </div>
                </div>
                <span className="hidden text-xs whitespace-nowrap text-muted-foreground sm:block">
                  {formatTime(job.updated)}
                </span>
              </Link>
            </li>
          ))}
        </ul>
      )}

      <Button asChild variant="ghost" size="sm" className="mt-3 w-full">
        <Link href="/jobs">
          View all jobs
          <ArrowRight className="ml-1 h-3 w-3" />
        </Link>
      </Button>
    </div>
  );
}
