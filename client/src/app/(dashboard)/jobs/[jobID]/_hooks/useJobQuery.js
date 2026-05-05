"use client";

import { useQuery } from "@tanstack/react-query";
import { ACTIVE_STATUSES, getJob } from "@/lib/sepex";

// Header query for the Job Detail card. Polls every 2s while the job is
// accepted or running so the status pill, elapsed timer, and dismiss-button
// gating stay live without a manual refresh. Stops polling for terminal
// states.
export default function useJobQuery(jobID) {
  return useQuery({
    queryKey: ["job", jobID],
    queryFn: () => getJob(jobID),
    enabled: Boolean(jobID),
    refetchInterval: (q) => {
      const status = q.state.data?.status;
      return ACTIVE_STATUSES.has(status) ? 2_000 : false;
    }
  });
}
