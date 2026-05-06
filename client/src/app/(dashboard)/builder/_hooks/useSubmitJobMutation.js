"use client";

import { useRouter } from "next/navigation";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { executeProcess } from "@/lib/sepex";

export default function useSubmitJobMutation() {
  const router = useRouter();
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: ({ processID, payload, async: isAsync, userEmail }) =>
      executeProcess(processID, {
        inputs: payload.inputs,
        tags: payload.tags,
        async: isAsync,
        userEmail
      }),
    onSuccess: (data, variables) => {
      queryClient.invalidateQueries({ queryKey: ["jobs"] });
      const jobID = data?.jobID;
      if (jobID) {
        toast.success(
          variables.async
            ? `Submitted — job ${jobID.slice(0, 8)}`
            : `Completed — job ${jobID.slice(0, 8)}`
        );
        router.push(`/jobs/${jobID}`);
      } else {
        toast.success("Submitted");
      }
    },
    onError: (err) => toast.error(err?.message || "Submission failed")
  });
}
