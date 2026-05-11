"use client";

import useJobMetadataQuery from "@/app/(dashboard)/jobs/[jobID]/_hooks/useJobMetadataQuery";

export default function MetadataTab({ jobID }) {
  const { data, isLoading, isError, error } = useJobMetadataQuery(jobID);

  if (isLoading) {
    return (
      <div className="text-sm text-muted-foreground">Loading metadata…</div>
    );
  }

  if (isError) {
    return (
      <div className="rounded-md border border-destructive/40 bg-destructive/5 p-3 text-sm text-destructive-fg">
        Couldn&rsquo;t load metadata:{" "}
        <span className="font-mono">{error?.message}</span>
      </div>
    );
  }

  if (!data || (typeof data === "object" && Object.keys(data).length === 0)) {
    return (
      <div className="rounded-md border border-border bg-muted/30 p-4 text-sm text-muted-foreground">
        No metadata for this job.
      </div>
    );
  }

  return (
    <pre className="overflow-x-auto rounded-lg bg-muted p-4 font-mono text-xs">
      {JSON.stringify(data, null, 2)}
    </pre>
  );
}
