"use client";

import { useQuery } from "@tanstack/react-query";
import { getJobMetadata } from "@/lib/sepex";

// Lazy fetch of the metadata blob. Powers both the Metadata tab and the
// Inputs tab — the latter looks for `metadata.inputs` since the OGC
// /jobs/{id} response doesn't echo the original submission payload.
export default function useJobMetadataQuery(jobID) {
  return useQuery({
    queryKey: ["job", jobID, "metadata"],
    queryFn: () => getJobMetadata(jobID),
    enabled: Boolean(jobID)
  });
}
