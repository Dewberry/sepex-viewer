"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import {
  ChevronDown,
  ChevronRight,
  Download,
  Search,
  Server,
  Terminal
} from "lucide-react";
import LogLine from "@/app/(dashboard)/jobs/[jobID]/_components/LogLine";
import useJobLogsQuery from "@/app/(dashboard)/jobs/[jobID]/_hooks/useJobLogsQuery";
import { groupProcessLogs } from "@/app/(dashboard)/jobs/[jobID]/_utils/groupLogs";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

const LEVELS = ["INFO", "WARN", "ERROR"];

export default function LogsTab({ jobID, jobStatus }) {
  const [stream, setStream] = useState("process");
  const [search, setSearch] = useState("");
  const [enabledLevels, setEnabledLevels] = useState(() => new Set(LEVELS));
  const [collapsed, setCollapsed] = useState(() => new Set());
  const [tail, setTail] = useState(true);
  const scrollRef = useRef(null);

  const { data, isLoading, isError, error } = useJobLogsQuery(jobID, {
    jobStatus
  });

  const allLogs = useMemo(
    () =>
      stream === "process" ? data?.process_logs || [] : data?.server_logs || [],
    [data, stream]
  );

  const groups = useMemo(() => {
    if (stream === "server") {
      return [{ title: "Server events", logs: allLogs }];
    }
    return groupProcessLogs(allLogs);
  }, [allLogs, stream]);

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

  // Tail-on-scroll: when the toggle is on, scroll the viewport to the bottom
  // any time the visible log count changes. Users who want to scroll back
  // through history just click Tail off.
  useEffect(() => {
    if (!tail) return;
    const el = scrollRef.current;
    if (!el) return;
    el.scrollTop = el.scrollHeight;
  }, [tail, flatFilteredCount, stream]);

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
            className={`inline-flex items-center gap-1 rounded px-3 py-1.5 text-xs font-medium transition-colors ${
              stream === "process"
                ? "bg-background text-foreground shadow-sm"
                : "text-muted-foreground hover:text-foreground"
            }`}
          >
            <Terminal className="h-3 w-3" />
            Process
          </button>
          <button
            type="button"
            onClick={() => setStream("server")}
            className={`inline-flex items-center gap-1 rounded px-3 py-1.5 text-xs font-medium transition-colors ${
              stream === "server"
                ? "bg-background text-foreground shadow-sm"
                : "text-muted-foreground hover:text-foreground"
            }`}
          >
            <Server className="h-3 w-3" />
            Server
          </button>
        </div>

        <div className="flex flex-wrap gap-2">
          {LEVELS.map((level) => {
            const active = enabledLevels.has(level);
            return (
              <button
                type="button"
                key={level}
                onClick={() => toggleLevel(level)}
                className={`rounded px-2 py-1 text-xs font-medium transition-colors ${
                  active
                    ? "bg-accent text-accent-foreground"
                    : "bg-muted text-muted-foreground/60 hover:bg-muted/80"
                }`}
                aria-pressed={active}
              >
                {level}
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
            onClick={() => setTail((t) => !t)}
            className={tail ? "bg-accent" : ""}
            aria-pressed={tail}
          >
            Tail
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
            <div className="p-4 text-destructive">
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
              return (
                <div
                  key={group.title}
                  className="border-b border-border last:border-0"
                >
                  <button
                    type="button"
                    onClick={() => toggleGroup(group.title)}
                    className="flex w-full items-center gap-2 bg-background px-3 py-2 text-left transition-colors hover:bg-accent"
                  >
                    {isCollapsed ? (
                      <ChevronRight className="h-4 w-4 text-muted-foreground" />
                    ) : (
                      <ChevronDown className="h-4 w-4 text-muted-foreground" />
                    )}
                    <span className="font-semibold">{group.title}</span>
                    <span className="text-muted-foreground">
                      ({group.logs.length})
                    </span>
                  </button>
                  {!isCollapsed ? (
                    <div className="space-y-0.5 p-2">
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

      <div className="text-xs text-muted-foreground">
        * Progress percentages parsed from{" "}
        <span className="font-mono">process_logs[]</span>
      </div>
    </div>
  );
}
