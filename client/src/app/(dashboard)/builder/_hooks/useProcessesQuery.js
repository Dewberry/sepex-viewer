"use client";

import { useQuery } from "@tanstack/react-query";
import { listProcesses } from "@/lib/sepex";

export default function useProcessesQuery() {
  return useQuery({
    queryKey: ["processes"],
    queryFn: () => listProcesses()
  });
}
