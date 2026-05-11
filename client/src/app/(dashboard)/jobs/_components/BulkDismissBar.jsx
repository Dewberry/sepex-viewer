"use client";

import { Trash2 } from "lucide-react";
import { Button } from "@/components/ui/button";

export default function BulkDismissBar({
  selectedCount,
  isPending,
  onDismiss,
  onClear
}) {
  if (selectedCount === 0) return null;

  return (
    <div className="flex items-center justify-between border-b border-border bg-accent px-4 py-2">
      <span className="text-sm font-medium">
        {selectedCount} selected
        <button
          type="button"
          onClick={onClear}
          className="ml-3 text-xs text-muted-foreground hover:underline"
        >
          Clear
        </button>
      </span>
      <Button
        variant="ghost"
        size="sm"
        disabled={isPending}
        onClick={onDismiss}
        className="gap-2 text-destructive-fg hover:text-destructive-fg"
      >
        <Trash2 className="h-4 w-4" />
        Dismiss {selectedCount}
      </Button>
    </div>
  );
}
