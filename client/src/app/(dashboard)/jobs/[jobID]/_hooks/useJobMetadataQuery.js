"use client";

import { useQuery } from "@tanstack/react-query";
import { getJobMetadata, SepexApiError } from "@/lib/sepex";

// Lazy fetch of the metadata blob. Powers both the Metadata tab and the
// Inputs tab — the latter looks for `metadata.inputs` since the OGC
// /jobs/{id} response doesn't echo the original submission payload.
//
// The Sepex API 404s for jobs without a metadata blob yet
// (failed/accepted/running/dismissed). Treat that as "empty" so the tabs
// render their empty states instead of an error banner.
export default function useJobMetadataQuery(jobID) {
  return useQuery({
    queryKey: ["job", jobID, "metadata"],
    queryFn: async () => {
      try {
        return await getJobMetadata(jobID);
      } catch (err) {
        if (err instanceof SepexApiError && err.status === 404) return null;
        throw err;
      }
    },
    enabled: Boolean(jobID)
  });
}
