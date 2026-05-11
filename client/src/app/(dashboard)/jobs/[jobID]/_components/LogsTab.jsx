"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import {
  ArrowDownToLine,
  ChevronDown,
  ChevronRight,
  Download,
  Search,
  Server,
  Terminal
} from "lucide-react";
import LogLine from "@/app/(dashboard)/jobs/[jobID]/_components/LogLine";
import useJobLogsQuery from "@/app/(dashboard)/jobs/[jobID]/_hooks/useJobLogsQuery";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

const LEVELS = ["INFO", "WARN", "ERROR"];

// Many compute plugins leave the structured `level` field empty and embed the
// level in the message itself. We've seen two prefix styles in the wild:
//   "[INFO] foo"             — LISFLOOD watchdog
//   "WARNING: foo" / "ERROR 45671: foo" — HMS / Java stack traces
// Pull either out so the level filter and color coding work.
const BRACKETED_LEVEL = /^\[(INFO|WARN(?:ING)?|ERROR)\] ?/i;
const INLINE_LEVEL = /^(INFO|WARN(?:ING)?|ERROR)\b[: ]?/i;

function normalizeLogEntry(entry) {
  const rawMsg = entry?.msg || "";
  let level = (entry?.level || "").toUpperCase();
  let msg = rawMsg;

  const bracket = rawMsg.match(BRACKETED_LEVEL);
  if (bracket) {
    msg = rawMsg.slice(bracket[0].length);
    if (!level) level = bracket[1].toUpperCase();
  } else if (!level) {
    const inline = rawMsg.match(INLINE_LEVEL);
    if (inline) level = inline[1].toUpperCase();
  }

  if (level === "WARNING") level = "WARN";
  if (!level) level = "INFO";
  return { ...entry, level, msg };
}

export default function LogsTab({ jobID, jobStatus }) {
  const [stream, setStream] = useState("process");
  const [search, setSearch] = useState("");
  const [enabledLevels, setEnabledLevels] = useState(() => new Set(LEVELS));
  const [collapsed, setCollapsed] = useState(() => new Set());
  const [autoScroll, setAutoScroll] = useState(true);
  const scrollRef = useRef(null);

  const { data, isLoading, isError, error } = useJobLogsQuery(jobID, {
    jobStatus
  });

  const allLogs = useMemo(() => {
    // Real Sepex returns `container_logs`; the mock layer returns `process_logs`.
    // Accept either so both backends render.
    const raw =
      stream === "process"
        ? data?.process_logs || data?.container_logs || []
        : data?.server_logs || [];
    return raw.map(normalizeLogEntry);
  }, [data, stream]);

  const levelCounts = useMemo(() => {
    const counts = { INFO: 0, WARN: 0, ERROR: 0 };
    for (const log of allLogs) {
      const level = (log.level || "INFO").toUpperCase();
      if (counts[level] !== undefined) counts[level] += 1;
    }
    return counts;
  }, [allLogs]);

  const groups = useMemo(
    () => [
      {
        title: stream === "server" ? "Server events" : "Process logs",
        logs: allLogs
      }
    ],
    [allLogs, stream]
  );

  const searchLower = search.toLowerCase();
  const filteredGroups = groups
    .map((g) => ({
      title: g.title,
      logs: g.logs.filter((log) => {
        const level = (log.level || "INFO").toUpperCase();
        if (!enabledLevels.has(level)) return false;
        if (searchLower && !(log.msg || "").toLowerCase().includes(searchLower))
          return false;
        return true;
      })
    }))
    .filter((g) => g.logs.length > 0);

  const flatFilteredCount = filteredGroups.reduce(
    (sum, g) => sum + g.logs.length,
    0
  );

  // Pin the viewport to the latest log whenever new lines arrive. Users who
  // want to scroll back through history just toggle Auto-scroll off.
  useEffect(() => {
    if (!autoScroll) return;
    const el = scrollRef.current;
    if (!el) return;
    el.scrollTop = el.scrollHeight;
  }, [autoScroll, flatFilteredCount, stream]);

  const toggleLevel = (level) => {
    setEnabledLevels((prev) => {
      const next = new Set(prev);
      if (next.has(level)) next.delete(level);
      else next.add(level);
      return next;
    });
  };

  const toggleGroup = (title) => {
    setCollapsed((prev) => {
      const next = new Set(prev);
      if (next.has(title)) next.delete(title);
      else next.add(title);
      return next;
    });
  };

  const handleDownload = () => {
    const lines = filteredGroups.flatMap((g) =>
      g.logs.map(
        (l) =>
          `[${(l.level || "INFO").toUpperCase()}] ${l.time || ""} ${l.msg || ""}`
      )
    );
    const blob = new Blob([lines.join("\n")], { type: "text/plain" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = `${jobID}-${stream}.log`;
    a.click();
    URL.revokeObjectURL(url);
  };

  return (
    <div className="space-y-4">
      <div className="flex flex-col gap-3 sm:flex-row sm:flex-wrap sm:items-center">
        <div className="flex gap-1 rounded-lg bg-muted p-1">
          <button
            type="button"
            onClick={() => setStream("process")}
            aria-pressed={stream === "process"}
            className={`inline-flex items-center gap-1 rounded px-3 py-1.5 text-xs font-medium transition-colors ${
              stream === "process"
                ? "bg-background text-foreground shadow-sm"
                : "text-muted-foreground hover:text-foreground"
            }`}
          >
            <Terminal className="h-3 w-3" aria-hidden="true" />
            Process
          </button>
          <button
            type="button"
            onClick={() => setStream("server")}
            aria-pressed={stream === "server"}
            className={`inline-flex items-center gap-1 rounded px-3 py-1.5 text-xs font-medium transition-colors ${
              stream === "server"
                ? "bg-background text-foreground shadow-sm"
                : "text-muted-foreground hover:text-foreground"
            }`}
          >
            <Server className="h-3 w-3" aria-hidden="true" />
            Server
          </button>
        </div>

        <div className="flex flex-wrap gap-2">
          {LEVELS.map((level) => {
            const active = enabledLevels.has(level);
            const count = levelCounts[level] ?? 0;
            return (
              <button
                type="button"
                key={level}
                onClick={() => toggleLevel(level)}
                disabled={count === 0}
                className={`inline-flex items-center gap-1.5 rounded px-2 py-1 text-xs font-medium transition-colors disabled:cursor-not-allowed disabled:opacity-50 ${
                  active
                    ? "bg-accent text-accent-foreground"
                    : "bg-muted text-muted-foreground hover:bg-muted/80"
                }`}
                aria-pressed={active}
                title={`${count} ${level.toLowerCase()} line${count === 1 ? "" : "s"} in this stream`}
              >
                <span>{level}</span>
                <span className="tabular-nums opacity-70">{count}</span>
              </button>
            );
          })}
        </div>

        <div className="relative min-w-[160px] flex-1">
          <Search className="pointer-events-none absolute top-1/2 left-2.5 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search logs…"
            className="pl-8"
          />
        </div>

        <div className="flex gap-2">
          <Button
            variant="outline"
            size="sm"
            onClick={() => setAutoScroll((v) => !v)}
            className={`gap-2 ${autoScroll ? "bg-accent" : ""}`}
            aria-pressed={autoScroll}
            title="Pin the viewport to the latest log as new lines arrive"
          >
            <ArrowDownToLine className="h-4 w-4" />
            Auto-scroll
          </Button>
          <Button
            variant="outline"
            size="sm"
            onClick={handleDownload}
            disabled={flatFilteredCount === 0}
            className="gap-2"
            title="Download visible logs"
          >
            <Download className="h-4 w-4" />
          </Button>
        </div>
      </div>

      <div className="overflow-hidden rounded-lg border border-border bg-muted">
        <div
          ref={scrollRef}
          className="max-h-[600px] overflow-y-auto font-mono text-xs"
        >
          {isLoading ? (
            <div className="p-4 text-muted-foreground">Loading logs…</div>
          ) : isError ? (
            <div className="p-4 text-destructive-fg">
              Couldn&rsquo;t load logs:{" "}
              <span className="font-mono">{error?.message}</span>
            </div>
          ) : flatFilteredCount === 0 ? (
            <div className="p-4 text-muted-foreground">
              {allLogs.length === 0
                ? "No logs yet."
                : "No log lines match the current filters."}
            </div>
          ) : (
            filteredGroups.map((group) => {
              const isCollapsed = collapsed.has(group.title);
              const regionId = `log-group-${group.title.replace(/\s+/g, "-")}`;
              return (
                <div
                  key={group.title}
                  className="border-b border-border last:border-0"
                >
                  <button
                    type="button"
                    onClick={() => toggleGroup(group.title)}
                    aria-expanded={!isCollapsed}
                    aria-controls={regionId}
                    className="flex w-full items-center gap-2 bg-background px-3 py-2 text-left transition-colors hover:bg-accent"
                  >
                    {isCollapsed ? (
                      <ChevronRight
                        className="h-4 w-4 text-muted-foreground"
                        aria-hidden="true"
                      />
                    ) : (
                      <ChevronDown
                        className="h-4 w-4 text-muted-foreground"
                        aria-hidden="true"
                      />
                    )}
                    <span className="font-semibold">{group.title}</span>
                    <span className="text-muted-foreground">
                      ({group.logs.length})
                    </span>
                  </button>
                  {!isCollapsed ? (
                    <div id={regionId} className="space-y-0.5 p-2">
                      {group.logs.map((log, i) => (
                        <LogLine key={i} entry={log} />
                      ))}
                    </div>
                  ) : null}
                </div>
              );
            })
          )}
        </div>
      </div>
    </div>
  );
}
