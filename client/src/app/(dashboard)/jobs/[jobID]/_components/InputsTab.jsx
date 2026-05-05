"use client";

import useJobMetadataQuery from "@/app/(dashboard)/jobs/[jobID]/_hooks/useJobMetadataQuery";

// The OGC `GET /jobs/{id}` response doesn't echo back the original inputs the
// user submitted via `POST /processes/{id}/execution`. They MAY survive on
// the metadata blob depending on the Sepex implementation — check there
// before showing the honest "not echoed yet" fallback.
function extractInputs(metadata) {
  if (!metadata || typeof metadata !== "object") return null;
  if (metadata.inputs) return { inputs: metadata.inputs, tags: metadata.tags };
  if (metadata.payload?.inputs) return metadata.payload;
  if (metadata.request?.inputs) return metadata.request;
  return null;
}

export default function InputsTab({ jobID }) {
  const { data, isLoading, isError, error } = useJobMetadataQuery(jobID);

  if (isLoading) {
    return <div className="text-sm text-muted-foreground">Loading inputs…</div>;
  }

  if (isError) {
    return (
      <div className="rounded-md border border-destructive/40 bg-destructive/5 p-3 text-sm text-destructive">
        Couldn&rsquo;t load inputs:{" "}
        <span className="font-mono">{error?.message}</span>
      </div>
    );
  }

  const payload = extractInputs(data);

  if (!payload) {
    return (
      <div className="space-y-2 rounded-md border border-border bg-muted/30 p-4 text-sm text-muted-foreground">
        <p>Original inputs aren&rsquo;t echoed back by the Sepex API yet.</p>
        <p className="text-xs">
          They were sent in{" "}
          <span className="font-mono">
            POST /processes/&#123;id&#125;/execution
          </span>{" "}
          but aren&rsquo;t persisted on the job record. Once the backend
          surfaces them — likely on{" "}
          <span className="font-mono">/jobs/&#123;id&#125;/metadata</span> —
          they&rsquo;ll appear here.
        </p>
      </div>
    );
  }

  return (
    <pre className="overflow-x-auto rounded-lg bg-muted p-4 font-mono text-xs">
      {JSON.stringify(payload, null, 2)}
    </pre>
  );
}
