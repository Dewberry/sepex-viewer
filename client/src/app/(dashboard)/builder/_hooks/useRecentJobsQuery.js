"use client";

import { useQuery } from "@tanstack/react-query";
import { listJobs } from "@/lib/sepex";

export default function useRecentJobsQuery(submitter) {
  return useQuery({
    queryKey: ["jobs", "recent", submitter],
    queryFn: () => listJobs({ limit: 10, submitter })
  });
}
