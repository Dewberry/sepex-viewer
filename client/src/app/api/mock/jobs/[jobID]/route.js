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
  setJobStatus
} from "@/app/api/mock/_lib/store";

export async function GET(request, { params }) {
  await maybeDelay(request);
  const { jobID } = await params;
  const job = findJob(jobID);
  if (!job) return notFound(`Job ${jobID} not found`);

  ensureProgressLogs(job);
  const projected = projectJob(job);

  return jsonResponse({
    type: "process",
    jobID: projected.jobID,
    created: projected.created,
    updated: projected.updated,
    status: projected.status,
    processID: projected.processID,
    submitter: projected.submitter,
    tags: projected.tags || [],
    host: projected.host,
    hostJobID: projected.hostJobID,
    mode: projected.mode,
    ...(projected.message ? { message: projected.message } : null),
    inputs: projected.inputs
  });
}

export async function DELETE(request, { params }) {
  await maybeDelay(request);
  const { jobID } = await params;
  const job = findJob(jobID);
  if (!job) return notFound(`Job ${jobID} not found`);

  const projected = projectJob(job);
  if (projected.status !== "accepted" && projected.status !== "running") {
    return badRequest(
      `Job ${jobID} cannot be dismissed (status=${projected.status})`
    );
  }

  setJobStatus(jobID, "dismissed");

  return jsonResponse({
    type: "process",
    jobID,
    processID: job.processID,
    status: "dismissed",
    message: `job ${jobID} dismissed`
  });
}
