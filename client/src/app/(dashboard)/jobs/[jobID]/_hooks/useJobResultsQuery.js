"use client";

import { useQuery } from "@tanstack/react-query";
import { getJobResults, SepexApiError } from "@/lib/sepex";

// One-shot fetch when the Results tab mounts. No polling — results don't
// change once the job reaches a terminal state, and watchers will be on the
// Logs tab while the job is still producing them.
//
// The Sepex API 404s for jobs that haven't produced results yet
// (failed/accepted/running/dismissed). Treat that as "empty" so the tab
// renders its empty state instead of an error banner.
export default function useJobResultsQuery(jobID) {
  return useQuery({
    queryKey: ["job", jobID, "results"],
    queryFn: async () => {
      try {
        return await getJobResults(jobID);
      } catch (err) {
        if (err instanceof SepexApiError && err.status === 404) return null;
        throw err;
      }
    },
    enabled: Boolean(jobID)
  });
}
