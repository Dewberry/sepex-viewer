"use client";

import { useQueries } from "@tanstack/react-query";
import extractErrorReason from "@/app/(dashboard)/dashboard/_utils/extractErrorReason";
import { getJobLogs } from "@/lib/sepex";

// Fans out one /jobs/{id}/logs query per failed job, each cached and refetched
// independently. Returns a map of jobID → { reason, isLoading, isError }.
export default function useFailedJobReasons(jobIDs) {
  const results = useQueries({
    queries: (jobIDs || []).map((jobID) => ({
      queryKey: ["job-logs", jobID],
      queryFn: () => getJobLogs(jobID),
      // Logs change less often than the job list; let them stay fresh longer
      // so opening / closing the dashboard doesn't restorm the API.
      staleTime: 60_000
    }))
  });

  const map = {};
  (jobIDs || []).forEach((jobID, idx) => {
    const r = results[idx];
    map[jobID] = {
      reason: r?.data ? extractErrorReason(r.data) : null,
      isLoading: r?.isLoading,
      isError: r?.isError
    };
  });
  return map;
}
