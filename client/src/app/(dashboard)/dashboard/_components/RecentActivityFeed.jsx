"use client";

import Link from "next/link";
import { ArrowRight } from "lucide-react";
import { getRelativeTime } from "@/app/(dashboard)/dashboard/_utils/relativeTime";
import { Skeleton } from "@/components/ui/skeleton";

const STATUS_TONE = {
  successful: "bg-status-successful text-white",
  failed: "bg-status-failed text-white",
  running: "bg-status-running text-white",
  accepted: "bg-status-accepted text-white",
  dismissed: "border border-status-dismissed text-status-dismissed",
  lost: "border border-status-lost text-status-lost"
};

function StatusPill({ status }) {
  return (
    <span
      className={`rounded-full px-2 py-0.5 text-[10px] font-medium ${
        STATUS_TONE[status] || "bg-muted text-muted-foreground"
      }`}
    >
      {status}
    </span>
  );
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
        <ul className="divide-y divide-border">
          {jobs.slice(0, limit).map((job) => (
            <li key={job.jobID}>
              <Link
                href={`/jobs/${job.jobID}`}
                className="-mx-2 flex items-center gap-3 rounded px-2 py-2 transition-colors hover:bg-accent/50"
              >
                <StatusPill status={job.status} />
                <div className="min-w-0 flex-1">
                  <div
                    className="truncate font-mono text-sm font-medium"
                    title={job.jobID}
                  >
                    {(job.jobID || "").slice(-8)}
                  </div>
                  <div className="truncate text-xs text-muted-foreground">
                    {job.processID || "—"}
                    {job.submitter ? <> · {job.submitter}</> : null}
                  </div>
                </div>
                <span className="hidden text-xs whitespace-nowrap text-muted-foreground sm:block">
                  {getRelativeTime(job.updated)}
                </span>
              </Link>
            </li>
          ))}
        </ul>
      )}

      <Link
        href="/jobs"
        className="mt-3 inline-flex items-center gap-1 text-xs text-dewberry-teal hover:underline"
      >
        View all jobs
        <ArrowRight className="h-3 w-3" />
      </Link>
    </div>
  );
}
