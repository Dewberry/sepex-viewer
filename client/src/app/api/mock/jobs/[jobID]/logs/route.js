import {
  badRequest,
  jsonResponse,
  maybeDelay,
  notFound
} from "@/app/api/mock/_lib/helpers";
import {
  ensureProgressLogs,
  findJob,
  projectJob,
  store
} from "@/app/api/mock/_lib/store";

export async function GET(request, { params }) {
  await maybeDelay(request);
  const { jobID } = await params;
  const job = findJob(jobID);
  if (!job) return notFound(`Job ${jobID} not found`);

  ensureProgressLogs(job);
  const projected = projectJob(job);

  if (projected.status === "accepted") {
    return badRequest(
      "Logs will be available after the job has reached running state"
    );
  }

  const entry = store.logs[jobID] || { process_logs: [], server_logs: [] };
  return jsonResponse({
    jobID,
    processID: projected.processID,
    status: projected.status,
    process_logs: entry.process_logs || [],
    server_logs: entry.server_logs || []
  });
}
