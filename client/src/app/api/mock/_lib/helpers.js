export const SCENARIO = process.env.MOCK_SCENARIO || "default";

export function commaSplit(value) {
  if (!value) return null;
  const list = String(value)
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean);
  return list.length ? list : null;
}

export function intParam(value, fallback) {
  const n = Number.parseInt(value ?? "", 10);
  return Number.isFinite(n) ? n : fallback;
}

export async function maybeDelay(request) {
  const url = new URL(request.url);
  const delay = intParam(url.searchParams.get("_delay"), 0);
  if (delay > 0) {
    await new Promise((r) => setTimeout(r, Math.min(delay, 5000)));
  }
}

export function maybeFailRandomly(routeName) {
  if (SCENARIO !== "error") return null;
  const FAIL_ROUTES = new Set([
    "GET /jobs",
    "GET /admin/resources",
    "GET /processes"
  ]);
  if (!FAIL_ROUTES.has(routeName)) return null;
  return Response.json(
    { error: "mock error scenario", code: 503 },
    { status: 503 }
  );
}

export function jsonResponse(body, init) {
  return Response.json(body, init);
}

export function notFound(detail = "Not found") {
  return Response.json({ error: detail, code: 404 }, { status: 404 });
}

export function badRequest(detail) {
  return Response.json({ error: detail, code: 400 }, { status: 400 });
}

export function buildJobsLinks(basePath, params, returnedCount, limit) {
  if (returnedCount < limit) return [];
  const next = new URLSearchParams();
  if (params.limit !== undefined) next.set("limit", String(params.limit));
  if (params.offset !== undefined)
    next.set("offset", String(Number(params.offset) + Number(params.limit)));
  for (const k of ["processID", "status", "submitter", "tags"]) {
    if (params[k]) next.set(k, params[k]);
  }
  return [{ href: `${basePath}?${next.toString()}`, title: "next" }];
}

export function nowIso() {
  return new Date().toISOString();
}
