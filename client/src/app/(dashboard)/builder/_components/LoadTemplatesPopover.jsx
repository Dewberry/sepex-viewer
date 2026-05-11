"use client";

import { useState } from "react";
import { FolderOpen, Trash2, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Popover,
  PopoverContent,
  PopoverTrigger
} from "@/components/ui/popover";
import { getCompactRelativeTime } from "@/lib/time";

export default function LoadTemplatesPopover({
  open,
  onOpenChange,
  templates,
  onUse,
  onDelete
}) {
  const [deletingId, setDeletingId] = useState(null);

  const sorted = [...templates].sort(
    (a, b) => new Date(b.savedAt).getTime() - new Date(a.savedAt).getTime()
  );

  return (
    <Popover open={open} onOpenChange={onOpenChange}>
      <PopoverTrigger asChild>
        <Button variant="outline" className="gap-2">
          <FolderOpen className="h-4 w-4" />
          Load
        </Button>
      </PopoverTrigger>
      <PopoverContent align="end" className="w-[360px] p-0">
        <div className="border-b px-4 py-3">
          <div className="mb-1 flex items-center justify-between">
            <h2 className="font-semibold">Saved templates</h2>
            <Button
              variant="ghost"
              size="icon"
              className="h-6 w-6"
              onClick={() => onOpenChange(false)}
              aria-label="Close"
            >
              <X className="h-4 w-4" />
            </Button>
          </div>
          <p className="text-xs italic text-muted-foreground">
            Saved in your browser. Other devices won&rsquo;t see these.
          </p>
        </div>
        <div className="max-h-[400px] overflow-y-auto">
          {sorted.length === 0 ? (
            <div className="px-4 py-8 text-center text-sm text-muted-foreground">
              No saved templates yet. Use Save to keep a payload around for
              later.
            </div>
          ) : (
            <div className="divide-y">
              {sorted.map((template) => (
                <div key={template.id} className="px-4 py-3">
                  {deletingId === template.id ? (
                    <div className="flex items-center gap-2">
                      <span className="text-sm">Delete?</span>
                      <Button
                        size="sm"
                        variant="outline"
                        onClick={() => {
                          onDelete(template.id);
                          setDeletingId(null);
                        }}
                        className="h-7 text-xs"
                      >
                        Confirm
                      </Button>
                      <Button
                        size="sm"
                        variant="ghost"
                        onClick={() => setDeletingId(null)}
                        className="h-7 text-xs"
                      >
                        Cancel
                      </Button>
                    </div>
                  ) : (
                    <div className="flex items-start gap-3">
                      <div className="min-w-0 flex-1">
                        <div className="mb-1 truncate text-sm font-medium">
                          {template.name}
                        </div>
                        <div className="mb-1 truncate text-xs text-muted-foreground">
                          {template.processID}
                        </div>
                        <div className="flex flex-wrap gap-1">
                          {(template.tags || []).slice(0, 3).map((tag) => (
                            <span
                              key={tag}
                              className="rounded bg-dewberry-teal/10 px-1.5 py-0.5 text-[10px] text-dewberry-teal"
                            >
                              {tag}
                            </span>
                          ))}
                          {(template.tags || []).length > 3 && (
                            <span className="rounded bg-muted px-1.5 py-0.5 text-[10px] text-muted-foreground">
                              +{template.tags.length - 3}
                            </span>
                          )}
                        </div>
                      </div>
                      <div className="flex flex-col items-end gap-1">
                        <span className="whitespace-nowrap text-xs text-muted-foreground">
                          {getCompactRelativeTime(template.savedAt)}
                        </span>
                        <div className="flex gap-1">
                          <Button
                            size="sm"
                            onClick={() => {
                              onUse(template);
                              onOpenChange(false);
                            }}
                            className="h-7 text-xs"
                          >
                            Use
                          </Button>
                          <Button
                            size="sm"
                            variant="ghost"
                            onClick={() => setDeletingId(template.id)}
                            className="h-7 w-7 p-0"
                            aria-label="Delete template"
                          >
                            <Trash2 className="h-3 w-3" />
                          </Button>
                        </div>
                      </div>
                    </div>
                  )}
                </div>
              ))}
            </div>
          )}
        </div>
      </PopoverContent>
    </Popover>
  );
}
