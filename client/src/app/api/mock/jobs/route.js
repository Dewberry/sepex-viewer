import {
  buildJobsLinks,
  commaSplit,
  intParam,
  jsonResponse,
  maybeDelay,
  maybeFailRandomly
} from "@/app/api/mock/_lib/helpers";
import { projectJobs, store } from "@/app/api/mock/_lib/store";
import { PROPOSED_API_ENABLED } from "@/lib/featureFlags";

// Real Sepex silently clamps limit at 100 (handlers.go:688). The proposed
// stats endpoint (item A) would aggregate over the full window — the higher
// cap here stands in for that while the flag is on.
const LIMIT_CAP = PROPOSED_API_ENABLED ? 5000 : 100;

export async function GET(request) {
  await maybeDelay(request);
  const fail = maybeFailRandomly("GET /jobs");
  if (fail) return fail;

  const url = new URL(request.url);
  const limit = Math.min(
    intParam(url.searchParams.get("limit"), 20),
    LIMIT_CAP
  );
  const offset = intParam(url.searchParams.get("offset"), 0);
  const processIDs = commaSplit(url.searchParams.get("processID"));
  const statuses = commaSplit(url.searchParams.get("status"));
  const submitters = commaSplit(url.searchParams.get("submitter"));
  const tagPrefixes = commaSplit(url.searchParams.get("tags"));

  // Flag-gated params from proposed-next-steps items C and I. Ignored when
  // the flag is off so the mock matches the real Sepex `/jobs` surface.
  const q = PROPOSED_API_ENABLED ? url.searchParams.get("q") : null;
  const updatedAfter = PROPOSED_API_ENABLED
    ? url.searchParams.get("updatedAfter")
    : null;
  const updatedBefore = PROPOSED_API_ENABLED
    ? url.searchParams.get("updatedBefore")
    : null;

  let projected = projectJobs(store.jobs);
  projected.sort(
    (a, b) => new Date(b.updated).getTime() - new Date(a.updated).getTime()
  );

  if (processIDs)
    projected = projected.filter((j) => processIDs.includes(j.processID));
  if (statuses)
    projected = projected.filter((j) => statuses.includes(j.status));
  if (submitters)
    projected = projected.filter((j) => submitters.includes(j.submitter));
  if (tagPrefixes) {
    projected = projected.filter((j) =>
      tagPrefixes.some((prefix) =>
        (j.tags || []).some((t) => t.startsWith(prefix))
      )
    );
  }
  if (q) {
    const needle = q.toLowerCase();
    projected = projected.filter(
      (j) =>
        (j.jobID || "").toLowerCase().includes(needle) ||
        (j.submitter || "").toLowerCase().includes(needle) ||
        (j.processID || "").toLowerCase().includes(needle) ||
        (j.tags || []).some((t) => t.toLowerCase().includes(needle))
    );
  }
  if (updatedAfter) {
    const t = new Date(updatedAfter).getTime();
    if (!Number.isNaN(t)) {
      projected = projected.filter((j) => new Date(j.updated).getTime() >= t);
    }
  }
  if (updatedBefore) {
    const t = new Date(updatedBefore).getTime();
    if (!Number.isNaN(t)) {
      projected = projected.filter((j) => new Date(j.updated).getTime() <= t);
    }
  }

  const total = projected.length;
  const slice = projected.slice(offset, offset + limit).map((j) => ({
    type: "process",
    jobID: j.jobID,
    processID: j.processID,
    status: j.status,
    submitter: j.submitter,
    updated: j.updated,
    tags: j.tags || [],
    // Item H: denormalized last error from failed-job logs. Sepex doesn't
    // ship this field today, so flag-gate it to match the real /jobs surface.
    ...(PROPOSED_API_ENABLED && j.status === "failed" && j.lastErrorMessage
      ? { lastErrorMessage: j.lastErrorMessage }
      : null)
  }));

  const links = buildJobsLinks(
    "/jobs",
    {
      limit,
      offset,
      processID: url.searchParams.get("processID"),
      status: url.searchParams.get("status"),
      submitter: url.searchParams.get("submitter"),
      tags: url.searchParams.get("tags")
    },
    slice.length,
    limit
  );

  const body = { jobs: slice, links };
  if (PROPOSED_API_ENABLED) {
    body.total = total;
    body.pages = Math.max(1, Math.ceil(total / limit));
  }
  return jsonResponse(body);
}
