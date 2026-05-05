import { formatDistanceToNow } from "date-fns";

export function getRelativeTime(dateString) {
  if (!dateString) return "—";
  try {
    return `${formatDistanceToNow(new Date(dateString))} ago`;
  } catch {
    return dateString;
  }
}

export function getLastUpdatedLabel(now, dataUpdatedAt) {
  if (!dataUpdatedAt) return "never";
  const sec = Math.max(0, Math.floor((now - dataUpdatedAt) / 1000));
  if (sec < 5) return "just now";
  if (sec < 60) return `${sec}s ago`;
  const min = Math.floor(sec / 60);
  if (min < 60) return `${min}m ago`;
  return `${Math.floor(min / 60)}h ago`;
}
