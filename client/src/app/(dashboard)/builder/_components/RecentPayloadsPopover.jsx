"use client";

import { ArrowRight, FolderOpen, X } from "lucide-react";
import StatusIcon from "@/components/sepex/StatusIcon";
import { Button } from "@/components/ui/button";
import {
  Popover,
  PopoverContent,
  PopoverTrigger
} from "@/components/ui/popover";
import { Skeleton } from "@/components/ui/skeleton";
import { getCompactRelativeTime } from "@/lib/time";

export default function RecentPayloadsPopover({
  open,
  onOpenChange,
  jobs,
  isLoading,
  isError,
  onUse,
  onViewAll
}) {
  return (
    <Popover open={open} onOpenChange={onOpenChange}>
      <PopoverTrigger asChild>
        <Button variant="outline" size="sm" className="gap-2">
          <FolderOpen className="h-4 w-4" />
          Recent payloads
        </Button>
      </PopoverTrigger>
      <PopoverContent align="end" className="w-[360px] p-0">
        <div className="flex items-center justify-between border-b px-4 py-3">
          <h3 className="font-semibold">Recent payloads</h3>
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
        <div className="max-h-[400px] overflow-y-auto">
          {isLoading ? (
            <div className="space-y-2 p-4">
              <Skeleton className="h-12 w-full" />
              <Skeleton className="h-12 w-full" />
              <Skeleton className="h-12 w-full" />
            </div>
          ) : isError ? (
            <div className="px-4 py-6 text-center text-sm text-muted-foreground">
              Couldn&rsquo;t reach the API.
            </div>
          ) : jobs.length === 0 ? (
            <div className="px-4 py-8 text-center text-sm text-muted-foreground">
              No recent payloads yet. Submit a job to see it here.
            </div>
          ) : (
            <div className="divide-y">
              {jobs.map((job) => (
                <button
                  type="button"
                  key={job.jobID}
                  onClick={() => onUse(job)}
                  className="w-full px-4 py-3 text-left transition-colors hover:bg-accent"
                >
                  <div className="flex items-start gap-3">
                    <StatusIcon status={job.status} />
                    <div className="min-w-0 flex-1">
                      <div className="mb-1 truncate text-sm font-medium">
                        {job.processID}
                      </div>
                      <div className="flex flex-wrap gap-1">
                        {(job.tags || []).map((tag) => (
                          <span
                            key={tag}
                            className="rounded bg-dewberry-teal/10 px-1.5 py-0.5 text-[10px] text-dewberry-teal"
                          >
                            {tag}
                          </span>
                        ))}
                      </div>
                    </div>
                    <div className="whitespace-nowrap text-xs text-muted-foreground">
                      {getCompactRelativeTime(job.updated)}
                    </div>
                  </div>
                </button>
              ))}
            </div>
          )}
        </div>
        <div className="border-t px-4 py-3">
          <button
            type="button"
            onClick={onViewAll}
            className="flex items-center gap-1 text-sm text-dewberry-teal hover:underline"
          >
            View all in Jobs
            <ArrowRight className="h-3 w-3" />
          </button>
        </div>
      </PopoverContent>
    </Popover>
  );
}
