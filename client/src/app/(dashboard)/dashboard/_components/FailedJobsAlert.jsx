"use client";

import Link from "next/link";
import { AlertTriangle, ArrowRight } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";
import { getCompactRelativeTime } from "@/lib/time";

const MAX_ROWS = 3;

export default function FailedJobsAlert({ jobs, isLoading, isError }) {
  if (isLoading) {
    return (
      <div className="rounded-lg border border-status-failed/30 bg-status-failed/5 p-4">
        <Skeleton className="h-5 w-32" />
        <div className="mt-3 space-y-2">
          <Skeleton className="h-14 w-full" />
          <Skeleton className="h-14 w-full" />
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

  const visible = jobs.slice(0, MAX_ROWS);
  const hasReasons = visible.some((j) => j.lastErrorMessage);

  return (
    <div className="rounded-lg border border-status-failed/30 bg-status-failed/5 p-4">
      <div className="mb-3 flex items-center gap-2">
        <AlertTriangle className="h-5 w-5 text-status-failed" />
        <h3 className="font-semibold text-status-failed">
          {jobs.length} Failed {jobs.length === 1 ? "Job" : "Jobs"}
        </h3>
      </div>

      <div className="space-y-2">
        {visible.map((job) => (
          <div
            key={job.jobID}
            className="flex items-center justify-between gap-3 rounded border border-border bg-card p-3 transition-shadow hover:shadow-md"
          >
            <div className="min-w-0 flex-1">
              <Link
                href={`/jobs/${job.jobID}`}
                className="block truncate font-mono text-sm font-medium hover:underline"
              >
                {job.jobID}
              </Link>
              <div className="truncate text-xs text-muted-foreground">
                {job.processID || "—"} · {job.submitter || "—"} ·{" "}
                {getCompactRelativeTime(job.updated) || "—"}
              </div>
              {job.lastErrorMessage ? (
                <div className="mt-1 truncate text-xs text-status-failed/90">
                  {job.lastErrorMessage}
                </div>
              ) : null}
            </div>
            <Button asChild variant="ghost" size="sm" className="text-xs">
              <Link href={`/jobs/${job.jobID}?tab=logs`}>View logs</Link>
            </Button>
          </div>
        ))}
      </div>

      <Button
        asChild
        variant="ghost"
        size="sm"
        className="mt-3 w-full text-status-failed hover:text-status-failed"
      >
        <Link href="/jobs?status=failed">
          View all failures
          <ArrowRight className="ml-1 h-3 w-3" />
        </Link>
      </Button>

      {hasReasons ? null : (
        <div className="mt-2 text-xs text-muted-foreground">
          Open a job to see why it failed.
        </div>
      )}
    </div>
  );
}
