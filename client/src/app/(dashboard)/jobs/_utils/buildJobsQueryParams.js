// Maps the page-local filter state + pagination into the shape that the Sepex
// `listJobs` client expects. The `search` field is intentionally excluded —
// the API has no ?q= yet, so search is applied client-side over the response.

export default function buildJobsQueryParams(filters, pageSize, offset) {
  return {
    limit: pageSize,
    offset,
    processID: filters.processID || undefined,
    status: filters.status || undefined,
    submitter: filters.submitter || undefined
  };
}
