"use client";

import { useState } from "react";
import LogsTab from "@/app/(dashboard)/jobs/[jobID]/_components/LogsTab";
import MessageBanner from "@/app/(dashboard)/jobs/[jobID]/_components/MessageBanner";
import MetadataTab from "@/app/(dashboard)/jobs/[jobID]/_components/MetadataTab";
import ResultsTab from "@/app/(dashboard)/jobs/[jobID]/_components/ResultsTab";
import RunSummaryCard from "@/app/(dashboard)/jobs/[jobID]/_components/RunSummaryCard";
import useJobQuery from "@/app/(dashboard)/jobs/[jobID]/_hooks/useJobQuery";
import { Skeleton } from "@/components/ui/skeleton";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";

// Shared content renderer for the Job Detail experience. Used by both
// `/jobs/[jobID]/page.jsx` (full-page deep link) and the Jobs drawer at
// `/jobs?selected=<id>`. Keep the public contract — `<JobDetailContent
// jobID="…" />` — stable; the drawer depends on it.
export default function JobDetailContent({ jobID }) {
  const [tab, setTab] = useState("logs");
  const jobQuery = useJobQuery(jobID);

  if (!jobID) return null;

  if (jobQuery.isLoading) {
    return (
      <div className="space-y-3">
        <Skeleton className="h-24 w-full rounded-lg" />
        <Skeleton className="h-9 w-64 rounded-lg" />
        <Skeleton className="h-72 w-full rounded-lg" />
      </div>
    );
  }

  if (jobQuery.isError) {
    return (
      <div className="rounded-md border border-destructive/40 bg-destructive/5 p-4 text-sm text-destructive-fg">
        Couldn&rsquo;t load job:{" "}
        <span className="font-mono">{jobQuery.error?.message}</span>
      </div>
    );
  }

  const job = jobQuery.data;
  if (!job) {
    return (
      <div className="text-sm text-muted-foreground">
        No job found for <span className="font-mono">{jobID}</span>.
      </div>
    );
  }

  return (
    <div className="space-y-4">
      <RunSummaryCard job={job} />
      <MessageBanner message={job.message} />
      <Tabs value={tab} onValueChange={setTab} className="gap-4">
        <TabsList>
          <TabsTrigger value="logs">Logs</TabsTrigger>
          <TabsTrigger value="results">Results</TabsTrigger>
          <TabsTrigger value="metadata">Metadata</TabsTrigger>
        </TabsList>
        <TabsContent value="logs">
          <LogsTab jobID={jobID} jobStatus={job.status} />
        </TabsContent>
        <TabsContent value="results">
          <ResultsTab jobID={jobID} jobStatus={job.status} />
        </TabsContent>
        <TabsContent value="metadata">
          <MetadataTab jobID={jobID} />
        </TabsContent>
      </Tabs>
    </div>
  );
}
