"use client";

import { useQuery } from "@tanstack/react-query";
import { getJobsLimitForRange } from "@/app/(dashboard)/dashboard/_utils/timeRange";
import { listJobs } from "@/lib/sepex";

// Single fetch that powers KPIs / charts / activity. No polling — the page
// header has a manual refresh button + the global 30s staleTime.
//
// 7d / 30d are best-effort: until the Sepex API ships ?updatedAfter /
// ?updatedBefore, we can only fetch the most-recent N jobs and let the page
// window them client-side.
export default function useDashboardJobs(range) {
  return useQuery({
    queryKey: ["dashboard", "jobs", range],
    queryFn: () => listJobs({ limit: getJobsLimitForRange(range) })
  });
}
