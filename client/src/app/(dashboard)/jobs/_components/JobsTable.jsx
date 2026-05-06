"use client";

import JobIdLink from "@/app/(dashboard)/jobs/_components/JobIdLink";
import StatusPill from "@/app/(dashboard)/jobs/_components/StatusPill";
import getRunName from "@/app/(dashboard)/jobs/_utils/getRunName";
import { getRelativeTime } from "@/app/(dashboard)/jobs/_utils/relativeTime";
import { Checkbox } from "@/components/ui/checkbox";
import { Skeleton } from "@/components/ui/skeleton";
import { ACTIVE_STATUSES } from "@/lib/sepex";

function TagChip({ tag }) {
  return (
    <span className="inline-flex items-center rounded-md border border-border bg-muted px-2 py-0.5 text-xs text-muted-foreground">
      {tag}
    </span>
  );
}

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
        <p className="font-medium text-destructive">
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
            {process.env.NEXT_PUBLIC_API_URL || "http://localhost:5050"}
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
              <th className="p-2 text-left">Run (from tags)</th>
              <th className="p-2 text-left">Submitter</th>
              <th className="p-2 text-left">Updated</th>
              <th className="p-2 text-left">Tags</th>
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
                  <td className="p-2 font-mono text-xs">
                    {getRunName(job.tags)}
                  </td>
                  <td className="p-2 text-sm">{job.submitter || "—"}</td>
                  <td
                    className="p-2 text-sm text-muted-foreground"
                    title={
                      job.updated ? new Date(job.updated).toUTCString() : ""
                    }
                  >
                    {getRelativeTime(job.updated)}
                  </td>
                  <td className="p-2">
                    <div className="flex flex-wrap gap-1">
                      {(job.tags || []).slice(0, 2).map((tag) => (
                        <TagChip key={tag} tag={tag} />
                      ))}
                      {(job.tags || []).length > 2 ? (
                        <TagChip tag={`+${(job.tags || []).length - 2}`} />
                      ) : null}
                    </div>
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
              <div className="mb-2 text-sm text-muted-foreground">
                {job.processID}
                {job.submitter ? <> · {job.submitter}</> : null}
              </div>
              <div className="flex flex-wrap gap-1">
                {(job.tags || []).map((tag) => (
                  <TagChip key={tag} tag={tag} />
                ))}
              </div>
            </div>
          );
        })}
      </div>
    </>
  );
}
