"use client";

import { useQuery } from "@tanstack/react-query";
import { listJobs } from "@/lib/sepex";

export default function useFailedJobs(limit = 5) {
  return useQuery({
    queryKey: ["dashboard", "failed-jobs", limit],
    queryFn: () => listJobs({ status: "failed", limit })
  });
}
