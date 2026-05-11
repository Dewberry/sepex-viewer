"use client";

import Link from "next/link";
import { ExternalLink } from "lucide-react";
import JobDetailContent from "@/app/(dashboard)/jobs/[jobID]/_components/JobDetailContent";
import {
  Sheet,
  SheetContent,
  SheetDescription,
  SheetTitle
} from "@/components/ui/sheet";

export default function JobDetailDrawer({ jobID, open, onOpenChange }) {
  return (
    <Sheet open={open} onOpenChange={onOpenChange}>
      <SheetContent
        side="right"
        className="flex w-full flex-col gap-0 p-0 sm:max-w-[720px]"
      >
        <SheetTitle className="sr-only">Job details</SheetTitle>
        <SheetDescription className="sr-only">
          Logs, results, and metadata for this job. Use the link above to open
          the full page.
        </SheetDescription>
        {jobID ? (
          <>
            <div className="flex items-center justify-between border-b border-border px-4 py-3">
              <Link
                href={`/jobs/${jobID}`}
                className="inline-flex items-center gap-1 text-sm text-dewberry-teal hover:underline"
              >
                Open full page
                <ExternalLink className="h-3 w-3" />
              </Link>
            </div>
            <div className="flex-1 overflow-y-auto px-4 py-6 lg:px-6">
              <JobDetailContent jobID={jobID} />
            </div>
          </>
        ) : null}
      </SheetContent>
    </Sheet>
  );
}
