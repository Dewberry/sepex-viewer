"use client";

import { Download } from "lucide-react";
import useJobResultsQuery from "@/app/(dashboard)/jobs/[jobID]/_hooks/useJobResultsQuery";
import { Button } from "@/components/ui/button";

// The Sepex API returns results in a few shapes depending on the backend:
//   • Async + downloadable files (hms-runner, dss_to_zarr):
//       { outputs: { links: [{href,title,type,rel}], results: [{href,title}] } }
//   • Sync + scalar (pyecho):
//       { outputs: "<echoed string>" }
//   • OGC array form:   [{ id|name, href, mediaType }, …]
//   • OGC map form:     { outputName: { href, mediaType } | "scalar", … }   (mock)
// Normalize defensively so the UI doesn't care.

function fromArray(arr) {
  return arr
    .map((entry) => {
      const href = entry.href || entry.value?.href;
      const type =
        entry.mediaType ||
        entry.type ||
        entry.value?.mediaType ||
        entry.value?.type;
      const value =
        href || type
          ? undefined
          : typeof entry.value !== "undefined"
            ? entry.value
            : undefined;
      return {
        name: entry.title || entry.id || entry.name || entry.key || "(unnamed)",
        href,
        type,
        value
      };
    })
    .filter((e) => e.href || e.type || typeof e.value !== "undefined");
}

function fromMap(map) {
  return Object.entries(map).map(([name, v]) => {
    if (v && typeof v === "object") {
      const href = v.href || v.value?.href;
      const type = v.mediaType || v.type || v.value?.mediaType || v.value?.type;
      const value = href
        ? undefined
        : typeof v.value !== "undefined"
          ? v.value
          : undefined;
      return { name, href, type, value };
    }
    // Scalar in the map (e.g. { message: "echoed string" })
    return { name, value: v };
  });
}

function normalizeResults(data) {
  if (!data) return [];
  if (Array.isArray(data)) return fromArray(data);
  if (typeof data !== "object") return [];

  // Real Sepex envelope.
  if ("outputs" in data) {
    const o = data.outputs;
    if (o === null || o === undefined) return [];
    // Scalar — pyecho-style sync response, single unnamed output.
    if (typeof o !== "object") return [{ name: "output", value: o }];
    if (Array.isArray(o)) return fromArray(o);
    // hms-runner-style: prefer presigned HTTPS links over raw s3:// uris.
    if (Array.isArray(o.links) && o.links.length > 0) return fromArray(o.links);
    if (Array.isArray(o.results) && o.results.length > 0)
      return fromArray(o.results);
    return fromMap(o);
  }

  // Legacy / mock layer: top-level object is itself the output map.
  return fromMap(data);
}

export default function ResultsTab({ jobID, jobStatus }) {
  const { data, isLoading, isError, error } = useJobResultsQuery(jobID);

  if (isLoading) {
    return (
      <div className="text-sm text-muted-foreground">Loading results…</div>
    );
  }

  if (isError) {
    return (
      <div className="rounded-md border border-destructive/40 bg-destructive/5 p-3 text-sm text-destructive-fg">
        Couldn&rsquo;t load results:{" "}
        <span className="font-mono">{error?.message}</span>
      </div>
    );
  }

  const entries = normalizeResults(data);

  if (entries.length === 0) {
    return (
      <div className="rounded-md border border-border bg-muted/30 p-4 text-sm text-muted-foreground">
        {jobStatus === "successful"
          ? "Job completed but produced no outputs."
          : "Job hasn’t produced results yet."}
      </div>
    );
  }

  return (
    <div className="space-y-3">
      {entries.map((entry) => {
        const hasValue = typeof entry.value !== "undefined";
        const displayValue = hasValue
          ? typeof entry.value === "object"
            ? JSON.stringify(entry.value, null, 2)
            : String(entry.value)
          : null;
        return (
          <div
            key={entry.name}
            className="rounded-lg border border-border bg-muted/30 p-4 transition-colors hover:bg-muted/50"
          >
            <div className="flex items-center justify-between gap-3">
              <div className="min-w-0 flex-1">
                <div className="font-semibold">{entry.name}</div>
                {entry.href ? (
                  <div className="truncate font-mono text-xs text-muted-foreground">
                    {entry.href}
                  </div>
                ) : null}
                {entry.type ? (
                  <div className="text-xs text-muted-foreground">
                    {entry.type}
                  </div>
                ) : null}
              </div>
              {entry.href ? (
                <Button
                  asChild
                  variant="ghost"
                  size="sm"
                  title="Open / download"
                >
                  <a
                    href={entry.href}
                    target="_blank"
                    rel="noreferrer noopener"
                  >
                    <Download className="h-4 w-4" />
                  </a>
                </Button>
              ) : null}
            </div>
            {hasValue ? (
              <pre className="mt-2 overflow-x-auto rounded bg-background/60 p-2 font-mono text-xs whitespace-pre-wrap">
                {displayValue}
              </pre>
            ) : null}
          </div>
        );
      })}
    </div>
  );
}
