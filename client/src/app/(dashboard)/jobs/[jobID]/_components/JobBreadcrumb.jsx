"use client";

import Link from "next/link";
import { ChevronRight } from "lucide-react";
import useJobQuery from "@/app/(dashboard)/jobs/[jobID]/_hooks/useJobQuery";

export default function JobBreadcrumb({ jobID }) {
  const { data: job } = useJobQuery(jobID);
  const processID = job?.processID;
  const shortID = jobID ? jobID.slice(-8) : "";

  return (
    <nav
      aria-label="Breadcrumb"
      className="flex items-center gap-1 text-sm text-muted-foreground"
    >
      <Link href="/jobs" className="transition-colors hover:text-foreground">
        Jobs
      </Link>
      {processID ? (
        <>
          <ChevronRight className="h-3.5 w-3.5" />
          <span>{processID}</span>
        </>
      ) : null}
      <ChevronRight className="h-3.5 w-3.5" />
      <span className="font-mono text-foreground">{shortID}</span>
    </nav>
  );
}
