"use client";

import { useQuery } from "@tanstack/react-query";
import { ACTIVE_STATUSES, getJobLogs } from "@/lib/sepex";

// Logs query. The 2s polling cadence matches the header so users watching a
// running job see new lines appear in near-real-time. The `jobStatus`
// argument lets the caller pass the live status from useJobQuery so polling
// stops the moment the job reaches a terminal state.
export default function useJobLogsQuery(jobID, { jobStatus } = {}) {
  const isActive = ACTIVE_STATUSES.has(jobStatus);
  return useQuery({
    queryKey: ["job", jobID, "logs"],
    queryFn: () => getJobLogs(jobID),
    enabled: Boolean(jobID),
    refetchInterval: isActive ? 2_000 : false
  });
}
