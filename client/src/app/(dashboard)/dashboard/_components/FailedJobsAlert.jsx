"use client";

import Link from "next/link";
import { AlertTriangle, ArrowRight } from "lucide-react";
import useFailedJobReasons from "@/app/(dashboard)/dashboard/_hooks/useFailedJobReasons";
import { getRelativeTime } from "@/app/(dashboard)/dashboard/_utils/relativeTime";
import { Skeleton } from "@/components/ui/skeleton";

function ReasonText({ entry }) {
  if (entry?.isLoading) {
    return <Skeleton className="h-3 w-40" />;
  }
  if (entry?.isError) {
    return <span className="text-muted-foreground">Logs unavailable</span>;
  }
  if (!entry?.reason) {
    return (
      <span className="text-muted-foreground">No ERROR entry in logs</span>
    );
  }
  return <span className="font-mono text-foreground/80">{entry.reason}</span>;
}

export default function FailedJobsAlert({ jobs, isLoading, isError }) {
  const jobIDs = (jobs || []).map((j) => j.jobID);
  const reasons = useFailedJobReasons(jobIDs);

  if (isLoading) {
    return (
      <div className="rounded-lg border border-status-failed/30 bg-status-failed/5 p-4">
        <Skeleton className="h-5 w-32" />
        <div className="mt-3 space-y-2">
          <Skeleton className="h-12 w-full" />
          <Skeleton className="h-12 w-full" />
        </div>
      </div>
    );
  }

  if (isError) {
    return (
      <div className="rounded-lg border border-status-failed/30 bg-status-failed/5 p-4">
        <div className="text-sm text-muted-foreground">
          Couldn&rsquo;t fetch failed jobs.
        </div>
      </div>
    );
  }

  if (!jobs || jobs.length === 0) {
    return null;
  }

  return (
    <div className="rounded-lg border border-status-failed/30 bg-status-failed/5 p-4">
      <div className="mb-3 flex items-center gap-2">
        <AlertTriangle className="h-5 w-5 text-status-failed" />
        <h3 className="font-semibold text-status-failed">
          {jobs.length} Failed {jobs.length === 1 ? "Job" : "Jobs"}
        </h3>
      </div>

      <div className="overflow-hidden rounded-md border border-border bg-card">
        <table className="w-full text-sm">
          <thead className="border-b border-border bg-muted text-xs tracking-wider text-muted-foreground uppercase">
            <tr>
              <th className="p-2 text-left">Job ID</th>
              <th className="p-2 text-left">Process</th>
              <th className="p-2 text-left">Submitter</th>
              <th className="p-2 text-left">Updated</th>
              <th className="p-2 text-left">Reason</th>
            </tr>
          </thead>
          <tbody>
            {jobs.map((job) => (
              <tr
                key={job.jobID}
                className="border-b border-border last:border-0 transition-colors hover:bg-muted/50"
              >
                <td className="p-2">
                  <Link
                    href={`/jobs/${job.jobID}`}
                    className="font-mono text-xs text-dewberry-teal hover:underline"
                  >
                    {job.jobID}
                  </Link>
                </td>
                <td className="p-2 text-xs">{job.processID || "—"}</td>
                <td className="p-2 text-xs">{job.submitter || "—"}</td>
                <td
                  className="p-2 text-xs text-muted-foreground"
                  title={job.updated ? new Date(job.updated).toUTCString() : ""}
                >
                  {getRelativeTime(job.updated)}
                </td>
                <td className="max-w-xs p-2 text-xs">
                  <div
                    className="truncate"
                    title={reasons[job.jobID]?.reason || ""}
                  >
                    <ReasonText entry={reasons[job.jobID]} />
                  </div>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="mt-3 flex items-center justify-between">
        <p className="text-xs text-muted-foreground">
          * Reason summarized from{" "}
          <span className="font-mono">/jobs/&#123;jobID&#125;/logs</span> (last
          ERROR-level entry)
        </p>
        <Link
          href="/jobs?status=failed"
          className="inline-flex items-center gap-1 text-xs text-red-700 hover:underline dark:text-red-400"
        >
          View all failures
          <ArrowRight className="h-3 w-3" />
        </Link>
      </div>
    </div>
  );
}
