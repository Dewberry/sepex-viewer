"use client";

import { useQuery } from "@tanstack/react-query";
import { getAdminResources } from "@/lib/sepex";

export default function useComputeResources() {
  return useQuery({
    queryKey: ["admin", "resources"],
    queryFn: () => getAdminResources(),
    refetchInterval: 5_000,
    staleTime: 0,
    select: (payload) => payload?.resources ?? payload
  });
}
