"use client";

// TODO: this is a minimal placeholder. The Job Detail handoff session will flesh
// it out — log filters/search/groups, tabs (Logs/Results/Metadata/Inputs),
// dismiss button, message banner, run summary stats, etc. Both the full Job
// Detail page and the Jobs drawer render this component.

import { useQuery } from "@tanstack/react-query";
import StatusPill from "@/app/(dashboard)/jobs/_components/StatusPill";
import { getRelativeTime } from "@/app/(dashboard)/jobs/_utils/relativeTime";
import { ACTIVE_STATUSES, getJob, getJobLogs } from "@/lib/sepex";

export default function JobDetailContent({ jobID }) {
  const jobQuery = useQuery({
    queryKey: ["job", jobID],
    queryFn: () => getJob(jobID),
    enabled: Boolean(jobID),
    refetchInterval: (q) => {
      const status = q.state.data?.status;
      return ACTIVE_STATUSES.has(status) ? 2_000 : false;
    }
  });

  const isActive = ACTIVE_STATUSES.has(jobQuery.data?.status);
  const logsQuery = useQuery({
    queryKey: ["job", jobID, "logs"],
    queryFn: () => getJobLogs(jobID),
    enabled: Boolean(jobID),
    refetchInterval: isActive ? 2_000 : false
  });

  if (!jobID) return null;

  const job = jobQuery.data;
  const logs = logsQuery.data;
  const processLogs = logs?.process_logs || [];
  const serverLogs = logs?.server_logs || [];

  if (jobQuery.isLoading) {
    return (
      <div className="space-y-3">
        <div className="h-6 w-32 animate-pulse rounded bg-muted" />
        <div className="h-4 w-full animate-pulse rounded bg-muted" />
        <div className="h-4 w-2/3 animate-pulse rounded bg-muted" />
      </div>
    );
  }

  if (jobQuery.isError) {
    return (
      <div className="rounded-md border border-destructive/40 bg-destructive/5 p-4 text-sm text-destructive">
        Couldn&rsquo;t load job:{" "}
        <span className="font-mono">{jobQuery.error?.message}</span>
      </div>
    );
  }

  if (!job) {
    return (
      <div className="text-sm text-muted-foreground">
        No job found for <span className="font-mono">{jobID}</span>.
      </div>
    );
  }

  return (
    <div className="space-y-6">
      <header className="space-y-2">
        <div className="flex items-center gap-3">
          <StatusPill status={job.status} />
          <span className="text-xs text-muted-foreground">
            updated {getRelativeTime(job.updated)}
          </span>
        </div>
        <div className="space-y-1">
          <div className="font-mono text-sm break-all">{job.jobID}</div>
          <div className="text-sm text-muted-foreground">
            <span className="text-foreground">{job.processID}</span>
            {job.submitter ? <> · {job.submitter}</> : null}
            {job.mode ? <> · {job.mode}</> : null}
          </div>
          {job.hostJobID ? (
            <div className="font-mono text-xs text-muted-foreground">
              {job.host ? `${job.host}://` : ""}
              {job.hostJobID}
            </div>
          ) : null}
        </div>
        {job.message ? (
          <div className="rounded-md border border-border bg-muted/40 p-3 text-sm">
            <span className="text-muted-foreground">Last update: </span>
            {job.message}
          </div>
        ) : null}
      </header>

      <section className="space-y-2">
        <h3 className="text-xs font-semibold tracking-wider text-muted-foreground uppercase">
          Process logs
        </h3>
        <pre className="max-h-[420px] overflow-auto rounded-md border border-border bg-muted/30 p-3 font-mono text-[11px] leading-relaxed">
          {logsQuery.isLoading
            ? "Loading logs…"
            : logsQuery.isError
              ? `Couldn't load logs: ${logsQuery.error?.message || ""}`
              : processLogs.length === 0
                ? "No process logs yet."
                : processLogs
                    .map(
                      (l) =>
                        `[${l.level || "INFO"}] ${l.time || ""} ${l.msg || ""}`
                    )
                    .join("\n")}
        </pre>
      </section>

      {serverLogs.length > 0 ? (
        <section className="space-y-2">
          <h3 className="text-xs font-semibold tracking-wider text-muted-foreground uppercase">
            Server logs
          </h3>
          <pre className="max-h-[260px] overflow-auto rounded-md border border-border bg-muted/30 p-3 font-mono text-[11px] leading-relaxed">
            {serverLogs
              .map(
                (l) => `[${l.level || "INFO"}] ${l.time || ""} ${l.msg || ""}`
              )
              .join("\n")}
          </pre>
        </section>
      ) : null}
    </div>
  );
}
