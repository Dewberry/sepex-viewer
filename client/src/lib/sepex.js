/**
 * Thin wrapper around the Sepex OGC API – Processes server.
 *
 * Endpoints (see DESIGN_BRIEF Appendix B):
 *   GET    /processes
 *   GET    /processes/{id}
 *   POST   /processes/{id}/execution
 *   GET    /jobs
 *   GET    /jobs/{id}
 *   GET    /jobs/{id}/logs
 *   GET    /jobs/{id}/results
 *   GET    /jobs/{id}/metadata
 *   DELETE /jobs/{id}
 *   GET    /admin/resources
 *
 * Submitter is identified via the X-SEPEX-User-Email header
 * (verified against ~/Documents/code/sepex/api/auth/keycloak.go).
 */

const DEFAULT_BASE_URL =
  process.env.NEXT_PUBLIC_SEPEX_BASE_URL || "http://localhost:5050";

const DEFAULT_USER_EMAIL =
  process.env.NEXT_PUBLIC_SEPEX_USER_EMAIL || "dev@dewberry.local";

export const JOB_STATUSES = [
  "accepted",
  "running",
  "successful",
  "failed",
  "dismissed",
  "lost"
];

export const ACTIVE_STATUSES = new Set(["accepted", "running"]);

class SepexApiError extends Error {
  constructor(message, { status, body } = {}) {
    super(message);
    this.name = "SepexApiError";
    this.status = status;
    this.body = body;
  }
}

function buildUrl(baseUrl, path, query) {
  const url = new URL(path.replace(/^\//, ""), baseUrl.replace(/\/?$/, "/"));
  if (query) {
    for (const [key, value] of Object.entries(query)) {
      if (value === undefined || value === null || value === "") continue;
      if (Array.isArray(value)) {
        for (const v of value) url.searchParams.append(key, v);
      } else {
        url.searchParams.set(key, value);
      }
    }
  }
  return url.toString();
}

async function request(
  path,
  {
    method = "GET",
    query,
    body,
    headers,
    baseUrl = DEFAULT_BASE_URL,
    userEmail = DEFAULT_USER_EMAIL,
    signal
  } = {}
) {
  const url = buildUrl(baseUrl, path, query);

  const finalHeaders = {
    Accept: "application/json",
    "X-SEPEX-User-Email": userEmail,
    ...(body !== undefined ? { "Content-Type": "application/json" } : null),
    ...headers
  };

  const res = await fetch(url, {
    method,
    headers: finalHeaders,
    body: body !== undefined ? JSON.stringify(body) : undefined,
    signal,
    cache: "no-store"
  });

  const contentType = res.headers.get("content-type") || "";
  const isJson = contentType.includes("application/json");
  const payload = isJson
    ? await res.json().catch(() => null)
    : await res.text().catch(() => "");

  if (!res.ok) {
    throw new SepexApiError(
      `Sepex ${method} ${path} failed: ${res.status} ${res.statusText}`,
      { status: res.status, body: payload }
    );
  }

  return payload;
}

// ── Processes ────────────────────────────────────────────────
export const listProcesses = (opts) => request("/processes", opts);
export const getProcess = (id, opts) => request(`/processes/${id}`, opts);

export const executeProcess = (id, { inputs, tags, async: isAsync, ...opts } = {}) =>
  request(`/processes/${id}/execution`, {
    ...opts,
    method: "POST",
    body: { inputs, ...(tags ? { tags } : null) },
    headers: {
      ...(isAsync ? { Prefer: "respond-async" } : null),
      ...(opts?.headers || {})
    }
  });

// ── Jobs ─────────────────────────────────────────────────────
export const listJobs = ({ limit, offset, processID, status, submitter, tags, ...opts } = {}) =>
  request("/jobs", {
    ...opts,
    query: { limit, offset, processID, status, submitter, tags }
  });

export const getJob = (jobID, opts) => request(`/jobs/${jobID}`, opts);
export const getJobLogs = (jobID, opts) => request(`/jobs/${jobID}/logs`, opts);
export const getJobResults = (jobID, opts) => request(`/jobs/${jobID}/results`, opts);
export const getJobMetadata = (jobID, opts) => request(`/jobs/${jobID}/metadata`, opts);
export const dismissJob = (jobID, opts) =>
  request(`/jobs/${jobID}`, { ...opts, method: "DELETE" });

// ── Admin ────────────────────────────────────────────────────
export const getAdminResources = (opts) => request("/admin/resources", opts);

export { SepexApiError };
