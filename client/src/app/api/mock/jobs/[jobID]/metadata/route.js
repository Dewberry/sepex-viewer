import {
  jsonResponse,
  maybeDelay,
  notFound
} from "@/app/api/mock/_lib/helpers";
import { findJob, projectJob, store } from "@/app/api/mock/_lib/store";

export async function GET(request, { params }) {
  await maybeDelay(request);
  const { jobID } = await params;
  const job = findJob(jobID);
  if (!job) return notFound(`Job ${jobID} not found`);

  const projected = projectJob(job);
  if (projected.status !== "successful") {
    return notFound(`Metadata not available (status=${projected.status})`);
  }

  const md = store.metadata[jobID];
  if (!md) return notFound("Metadata not available");
  return jsonResponse(md);
}
