"use client";

import { Download } from "lucide-react";
import useJobResultsQuery from "@/app/(dashboard)/jobs/[jobID]/_hooks/useJobResultsQuery";
import { Button } from "@/components/ui/button";

// The Sepex API returns results in a few shapes depending on the backend:
//   • Real Sepex:       { outputs: { links: [{href,title,type,rel}], results: [{href,title}] } }
//   • OGC array form:   [{ id|name, href, mediaType }, …]
//   • OGC map form:     { outputName: { href, mediaType }, … }   (current mock layer)
// Normalize defensively so the UI doesn't care.
function fromArray(arr) {
  return arr
    .map((entry) => ({
      name: entry.title || entry.id || entry.name || entry.key || "(unnamed)",
      href: entry.href || entry.value?.href,
      type:
        entry.mediaType ||
        entry.type ||
        entry.value?.mediaType ||
        entry.value?.type
    }))
    .filter((e) => e.href || e.type);
}

function normalizeResults(data) {
  if (!data) return [];
  if (Array.isArray(data)) return fromArray(data);
  if (typeof data !== "object") return [];

  // Real Sepex envelope. Prefer `links` (presigned HTTPS URLs the browser can
  // open) over `results` (s3:// URIs that need credentials).
  if (data.outputs && typeof data.outputs === "object") {
    if (Array.isArray(data.outputs.links) && data.outputs.links.length > 0) {
      return fromArray(data.outputs.links);
    }
    if (
      Array.isArray(data.outputs.results) &&
      data.outputs.results.length > 0
    ) {
      return fromArray(data.outputs.results);
    }
    // Fall through to treat data.outputs as an OGC map.
    return Object.entries(data.outputs).map(([name, value]) => ({
      name,
      href: value?.href,
      type: value?.mediaType || value?.type
    }));
  }

  return Object.entries(data).map(([name, value]) => ({
    name,
    href: value?.href,
    type: value?.mediaType || value?.type
  }));
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
      {entries.map((entry) => (
        <div
          key={entry.name}
          className="flex items-center justify-between gap-3 rounded-lg border border-border bg-muted/30 p-4 transition-colors hover:bg-muted/50"
        >
          <div className="min-w-0 flex-1">
            <div className="font-semibold">{entry.name}</div>
            {entry.href ? (
              <div className="truncate font-mono text-xs text-muted-foreground">
                {entry.href}
              </div>
            ) : null}
            {entry.type ? (
              <div className="text-xs text-muted-foreground">{entry.type}</div>
            ) : null}
          </div>
          {entry.href ? (
            <Button asChild variant="ghost" size="sm" title="Open / download">
              <a href={entry.href} target="_blank" rel="noreferrer noopener">
                <Download className="h-4 w-4" />
              </a>
            </Button>
          ) : null}
        </div>
      ))}
    </div>
  );
}
