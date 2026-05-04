"use client";

import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { listJobs } from "@/lib/sepex";

export default function useJobsQuery(params) {
  return useQuery({
    queryKey: ["jobs", params],
    queryFn: () => listJobs(params),
    placeholderData: keepPreviousData,
    refetchInterval: (query) => {
      const jobs = query.state.data?.jobs || [];
      return jobs.some((j) => j.status === "running" || j.status === "accepted")
        ? 15_000
        : false;
    }
  });
}
