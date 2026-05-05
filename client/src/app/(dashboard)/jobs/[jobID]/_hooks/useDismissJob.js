"use client";

import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { dismissJob } from "@/lib/sepex";

export default function useDismissJob(jobID) {
  const queryClient = useQueryClient();
  return useMutation({
    mutationFn: () => dismissJob(jobID),
    onSuccess: () => {
      toast.success("Job dismissed");
      queryClient.invalidateQueries({ queryKey: ["job", jobID] });
      queryClient.invalidateQueries({ queryKey: ["jobs"] });
    },
    onError: (err) => {
      toast.error(err?.message || "Failed to dismiss job");
    }
  });
}
