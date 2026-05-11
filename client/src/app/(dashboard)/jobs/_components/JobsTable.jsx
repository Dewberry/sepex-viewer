"use client";

import JobIdLink from "@/app/(dashboard)/jobs/_components/JobIdLink";
import StatusPill from "@/components/sepex/StatusPill";
import { Checkbox } from "@/components/ui/checkbox";
import { Skeleton } from "@/components/ui/skeleton";
import { ACTIVE_STATUSES } from "@/lib/sepex";
import { getRelativeTime } from "@/lib/time";

export default function JobsTable({
  jobs,
  isLoading,
  isError,
  error,
  hasActiveFilters,
  selectedJobID,
  onOpenJob,
  selectedIDs,
  onToggleRow,
  onToggleAll
}) {
  const dismissable = jobs.filter((j) => ACTIVE_STATUSES.has(j.status));
  const allDismissableSelected =
    dismissable.length > 0 &&
    dismissable.every((j) => selectedIDs.includes(j.jobID));

  if (isLoading && jobs.length === 0) {
    return (
      <div className="space-y-2 p-4">
        <Skeleton className="h-8 w-full" />
        <Skeleton className="h-8 w-full" />
        <Skeleton className="h-8 w-full" />
        <Skeleton className="h-8 w-full" />
      </div>
    );
  }

  if (isError) {
    return (
      <div className="border-b border-destructive/30 bg-destructive/5 p-3 text-sm">
        <p className="font-medium text-destructive-fg">
          Couldn&rsquo;t reach the API
          {error?.message ? (
            <>
              :{" "}
              <span className="font-mono text-xs font-normal">
                {error.message}
              </span>
            </>
          ) : null}
        </p>
        <p className="mt-1 text-xs text-muted-foreground">
          Make sure the Sepex server is running at{" "}
          <span className="font-mono">
            {process.env.NEXT_PUBLIC_API_URL ||
              process.env.NEXT_PUBLIC_SEPEX_BASE_URL ||
              "http://localhost:5050"}
          </span>
          .
        </p>
      </div>
    );
  }

  if (jobs.length === 0) {
    return (
      <div className="p-10 text-center text-sm text-muted-foreground">
        {hasActiveFilters
          ? "No jobs match these filters."
          : "No jobs have been submitted yet."}
      </div>
    );
  }

  return (
    <>
      <div className="hidden overflow-x-auto md:block">
        <table className="w-full">
          <thead className="border-b border-border bg-muted text-xs tracking-wider uppercase">
            <tr>
              <th className="w-8 p-2">
                <Checkbox
                  checked={allDismissableSelected}
                  onCheckedChange={(c) => onToggleAll(Boolean(c))}
                  aria-label="Select all dismissable jobs"
                />
              </th>
              <th className="p-2 text-left">Status</th>
              <th className="p-2 text-left">Job ID</th>
              <th className="p-2 text-left">Process</th>
              <th className="p-2 text-left">Submitter</th>
              <th className="p-2 text-left">Updated</th>
            </tr>
          </thead>
          <tbody>
            {jobs.map((job) => {
              const canDismiss = ACTIVE_STATUSES.has(job.status);
              const isSelected = selectedIDs.includes(job.jobID);
              const isOpen = selectedJobID === job.jobID;
              return (
                <tr
                  key={job.jobID}
                  className={`border-b border-border transition-colors ${
                    isOpen
                      ? "bg-accent/60"
                      : isSelected
                        ? "bg-accent/30"
                        : "hover:bg-muted/50"
                  }`}
                >
                  <td className="p-2">
                    <Checkbox
                      checked={isSelected}
                      disabled={!canDismiss}
                      onCheckedChange={(c) =>
                        onToggleRow(job.jobID, Boolean(c))
                      }
                      aria-label={`Select ${job.jobID}`}
                    />
                  </td>
                  <td className="p-2">
                    <StatusPill status={job.status} />
                  </td>
                  <td className="p-2">
                    <JobIdLink jobID={job.jobID} onClick={onOpenJob} />
                  </td>
                  <td className="p-2 text-sm">{job.processID}</td>
                  <td className="p-2 text-sm">{job.submitter || "—"}</td>
                  <td
                    className="p-2 text-sm text-muted-foreground"
                    title={
                      job.updated ? new Date(job.updated).toUTCString() : ""
                    }
                  >
                    {getRelativeTime(job.updated)}
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>

      <div className="divide-y divide-border md:hidden">
        {jobs.map((job) => {
          const canDismiss = ACTIVE_STATUSES.has(job.status);
          const isSelected = selectedIDs.includes(job.jobID);
          const isOpen = selectedJobID === job.jobID;
          return (
            <div
              key={job.jobID}
              className={`p-4 transition-colors ${isOpen ? "bg-accent/60" : ""}`}
            >
              <div className="mb-2 flex items-start justify-between gap-2">
                <div className="flex items-center gap-2">
                  <Checkbox
                    checked={isSelected}
                    disabled={!canDismiss}
                    onCheckedChange={(c) => onToggleRow(job.jobID, Boolean(c))}
                    aria-label={`Select ${job.jobID}`}
                  />
                  <StatusPill status={job.status} />
                </div>
                <span className="text-xs text-muted-foreground">
                  {getRelativeTime(job.updated)}
                </span>
              </div>
              <div className="mb-1">
                <JobIdLink jobID={job.jobID} onClick={onOpenJob} />
              </div>
              <div className="text-sm text-muted-foreground">
                {job.processID}
                {job.submitter ? <> · {job.submitter}</> : null}
              </div>
            </div>
          );
        })}
      </div>
    </>
  );
}
