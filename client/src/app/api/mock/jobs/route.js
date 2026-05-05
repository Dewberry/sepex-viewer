import {
  buildJobsLinks,
  commaSplit,
  intParam,
  jsonResponse,
  maybeDelay,
  maybeFailRandomly
} from "@/app/api/mock/_lib/helpers";
import { projectJobs, store } from "@/app/api/mock/_lib/store";

export async function GET(request) {
  await maybeDelay(request);
  const fail = maybeFailRandomly("GET /jobs");
  if (fail) return fail;

  const url = new URL(request.url);
  const limit = Math.min(intParam(url.searchParams.get("limit"), 20), 100);
  const offset = intParam(url.searchParams.get("offset"), 0);
  const processIDs = commaSplit(url.searchParams.get("processID"));
  const statuses = commaSplit(url.searchParams.get("status"));
  const submitters = commaSplit(url.searchParams.get("submitter"));
  const tagPrefixes = commaSplit(url.searchParams.get("tags"));

  let projected = projectJobs(store.jobs);
  // Most-recent first.
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

  const slice = projected.slice(offset, offset + limit).map((j) => ({
    type: "process",
    jobID: j.jobID,
    processID: j.processID,
    status: j.status,
    submitter: j.submitter,
    updated: j.updated,
    tags: j.tags || []
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

  return jsonResponse({ jobs: slice, links });
}
