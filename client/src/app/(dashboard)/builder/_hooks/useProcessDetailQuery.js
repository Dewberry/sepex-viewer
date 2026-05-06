"use client";

import { useQuery } from "@tanstack/react-query";
import { getProcess } from "@/lib/sepex";

export default function useProcessDetailQuery(processId) {
  return useQuery({
    queryKey: ["process", processId],
    queryFn: () => getProcess(processId),
    enabled: Boolean(processId)
  });
}
