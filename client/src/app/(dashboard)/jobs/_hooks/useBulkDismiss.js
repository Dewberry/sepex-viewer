"use client";

import { useState } from "react";
import { useMutation, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import { ACTIVE_STATUSES, dismissJob } from "@/lib/sepex";

export default function useBulkDismiss(visibleJobs) {
  const queryClient = useQueryClient();
  const [selectedIDs, setSelectedIDs] = useState([]);

  const toggleRow = (jobID, checked) =>
    setSelectedIDs((prev) =>
      checked ? [...prev, jobID] : prev.filter((id) => id !== jobID)
    );

  const toggleAll = (checked) => {
    if (!checked) {
      setSelectedIDs([]);
      return;
    }
    setSelectedIDs(
      visibleJobs
        .filter((j) => ACTIVE_STATUSES.has(j.status))
        .map((j) => j.jobID)
    );
  };

  const clear = () => setSelectedIDs([]);

  const mutation = useMutation({
    mutationFn: async (ids) => {
      const results = await Promise.allSettled(ids.map((id) => dismissJob(id)));
      return results.map((r, i) => ({ id: ids[i], result: r }));
    },
    onSuccess: (results) => {
      const ok = results.filter((r) => r.result.status === "fulfilled").length;
      const fail = results.length - ok;
      if (ok > 0) toast.success(`Dismissed ${ok} job${ok === 1 ? "" : "s"}`);
      if (fail > 0)
        toast.error(`Failed to dismiss ${fail} job${fail === 1 ? "" : "s"}`);
      setSelectedIDs([]);
      queryClient.invalidateQueries({ queryKey: ["jobs"] });
    },
    onError: (err) => toast.error(err?.message || "Dismiss failed")
  });

  const dismiss = () => {
    if (selectedIDs.length === 0) return;
    mutation.mutate(selectedIDs);
  };

  return {
    selectedIDs,
    toggleRow,
    toggleAll,
    clear,
    dismiss,
    isPending: mutation.isPending
  };
}
