"use client";

import { X } from "lucide-react";
import { Button } from "@/components/ui/button";

function Chip({ label, onRemove }) {
  return (
    <span className="inline-flex items-center gap-1 rounded-md bg-accent px-2 py-1 text-xs text-accent-foreground">
      {label}
      <button
        type="button"
        onClick={onRemove}
        className="rounded-sm p-0.5 hover:bg-muted"
        aria-label={`Remove ${label}`}
      >
        <X className="h-3 w-3" />
      </button>
    </span>
  );
}

export default function AppliedFiltersBar({ filters, onChange, onClearAll }) {
  const hasAny =
    filters.search || filters.processID || filters.status || filters.submitter;

  if (!hasAny) return null;

  return (
    <div className="flex flex-wrap items-center gap-2">
      {filters.search ? (
        <Chip
          label={`Search: ${filters.search}`}
          onRemove={() => onChange({ ...filters, search: "" })}
        />
      ) : null}
      {filters.processID ? (
        <Chip
          label={`Process: ${filters.processID}`}
          onRemove={() => onChange({ ...filters, processID: "" })}
        />
      ) : null}
      {filters.status ? (
        <Chip
          label={`Status: ${filters.status}`}
          onRemove={() => onChange({ ...filters, status: "" })}
        />
      ) : null}
      {filters.submitter ? (
        <Chip
          label={`Submitter: ${filters.submitter}`}
          onRemove={() => onChange({ ...filters, submitter: "" })}
        />
      ) : null}
      <Button
        variant="ghost"
        size="sm"
        onClick={onClearAll}
        className="ml-auto h-7 text-xs"
      >
        Clear all
      </Button>
    </div>
  );
}
