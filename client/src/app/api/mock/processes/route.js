import {
  intParam,
  jsonResponse,
  maybeDelay,
  maybeFailRandomly
} from "@/app/api/mock/_lib/helpers";
import { store } from "@/app/api/mock/_lib/store";

export async function GET(request) {
  await maybeDelay(request);
  const fail = maybeFailRandomly("GET /processes");
  if (fail) return fail;

  const url = new URL(request.url);
  const limit = Math.min(intParam(url.searchParams.get("limit"), 20), 100);
  const offset = intParam(url.searchParams.get("offset"), 0);

  // List view returns process descriptors only — inputs/outputs schema is
  // per-process detail (`GET /processes/{id}`).
  const all = store.processes.map((p) => ({
    version: p.version,
    id: p.id,
    title: p.title,
    description: p.description,
    jobControlOptions: p.jobControlOptions,
    outputTransmission: p.outputTransmission
  }));
  const slice = all.slice(offset, offset + limit);

  const links = [];
  if (slice.length === limit && offset + limit < all.length) {
    const next = new URLSearchParams({
      limit: String(limit),
      offset: String(offset + limit)
    });
    links.push({ href: `/processes?${next.toString()}`, title: "next" });
  }

  return jsonResponse({ processes: slice, links });
}
