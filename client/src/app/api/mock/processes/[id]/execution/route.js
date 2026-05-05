import {
  badRequest,
  jsonResponse,
  maybeDelay
} from "@/app/api/mock/_lib/helpers";
import { appendJob, store } from "@/app/api/mock/_lib/store";

export async function POST(request, { params }) {
  await maybeDelay(request);
  const { id } = await params;

  const proc = store.processes.find((p) => p.id === id);
  if (!proc) return badRequest(`Process ${id} not found`);

  let body;
  try {
    body = await request.json();
  } catch {
    return badRequest("Invalid JSON body");
  }

  const inputs = body?.inputs || {};
  const tags = Array.isArray(body?.tags) ? body.tags : [];
  const submitter =
    request.headers.get("x-sepex-user-email") || "dev@dewberry.local";
  const prefer = (request.headers.get("prefer") || "").toLowerCase();
  const isAsync = !prefer.includes("respond-sync");

  const jobID = crypto.randomUUID();
  const now = Date.now();
  const job = {
    type: "process",
    jobID,
    processID: id,
    status: isAsync ? "accepted" : "successful",
    submitter,
    created: new Date(now).toISOString(),
    updated: new Date(now).toISOString(),
    tags,
    host: "docker",
    hostJobID: `container-${jobID.slice(0, 8)}`,
    mode: isAsync ? "async" : "sync",
    inputs,
    _isMockSubmission: isAsync,
    _submittedAt: now,
    _terminalStatus: "successful"
  };
  appendJob(job);

  if (isAsync) {
    return jsonResponse(
      { type: "process", jobID, processID: id, status: "accepted", tags },
      {
        status: 201,
        headers: { "Preference-Applied": "respond-async" }
      }
    );
  }
  // sync: pretend the job ran instantly
  return jsonResponse({
    jobID,
    outputs: { stub: { value: "sync-mode-not-used-by-ui" } },
    tags
  });
}
