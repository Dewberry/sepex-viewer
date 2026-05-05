// Pulls the last ERROR-level entry from a /jobs/{id}/logs payload's
// process_logs[]. The Sepex API doesn't return a "reason" field — failure
// reason text is derived client-side from the logs.
export default function extractErrorReason(logs) {
  const entries = logs?.process_logs || [];
  for (let i = entries.length - 1; i >= 0; i -= 1) {
    const entry = entries[i];
    const level = (entry?.level || "").toUpperCase();
    if (level === "ERROR" && entry?.msg) {
      return entry.msg.trim();
    }
  }
  return null;
}
