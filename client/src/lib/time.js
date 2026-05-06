import { formatDistanceToNow } from "date-fns";

// Verbose relative time, e.g. "about 5 minutes ago". Used in tables and
// detail cards where there's room for natural-language phrasing.
export function getRelativeTime(dateString) {
  if (!dateString) return "—";
  try {
    return `${formatDistanceToNow(new Date(dateString))} ago`;
  } catch {
    return dateString;
  }
}

// Compact relative time, e.g. "12m ago" / "2h ago" / "3d ago". Used in
// popovers and dense lists where the verbose form crowds the layout.
export function getCompactRelativeTime(dateString) {
  if (!dateString) return "";
  const ms = Date.now() - new Date(dateString).getTime();
  const sec = Math.max(1, Math.floor(ms / 1000));
  if (sec < 60) return `${sec}s ago`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m ago`;
  const hr = Math.floor(min / 60);
  if (hr < 24) return `${hr}h ago`;
  return `${Math.floor(hr / 24)}d ago`;
}

// Sub-minute precision label for the "Last updated" indicator that ticks every
// second. Inputs are millisecond epochs (e.g. Date.now() and TanStack Query's
// dataUpdatedAt) so the caller can pass a stable reference.
export function getLastUpdatedLabel(now, dataUpdatedAt) {
  if (!dataUpdatedAt) return "never";
  const sec = Math.max(0, Math.floor((now - dataUpdatedAt) / 1000));
  if (sec < 5) return "just now";
  if (sec < 60) return `${sec}s ago`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m ago`;
  return `${Math.floor(min / 60)}h ago`;
}
