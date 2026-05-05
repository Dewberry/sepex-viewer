"use client";

import { useQuery } from "@tanstack/react-query";
import { getJobResults } from "@/lib/sepex";

// One-shot fetch when the Results tab mounts. No polling — results don't
// change once the job reaches a terminal state, and watchers will be on the
// Logs tab while the job is still producing them.
export default function useJobResultsQuery(jobID) {
  return useQuery({
    queryKey: ["job", jobID, "results"],
    queryFn: () => getJobResults(jobID),
    enabled: Boolean(jobID)
  });
}
