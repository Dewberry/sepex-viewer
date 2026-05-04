"use client";

import { useMemo, useState } from "react";
import { ChevronDown, Search } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Skeleton } from "@/components/ui/skeleton";

const API_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:5050";

export default function ProcessPicker({
  processes,
  selected,
  isLoading,
  isError,
  onSelect
}) {
  const [open, setOpen] = useState(false);
  const [search, setSearch] = useState("");

  const filtered = useMemo(() => {
    const q = search.toLowerCase();
    return processes.filter(
      (p) =>
        (p.title || "").toLowerCase().includes(q) ||
        (p.id || "").toLowerCase().includes(q)
    );
  }, [processes, search]);

  const handleSelect = (id) => {
    setOpen(false);
    setSearch("");
    onSelect(id);
  };

  return (
    <div className="relative">
      <Button
        type="button"
        variant="outline"
        onClick={() => setOpen((v) => !v)}
        className="h-auto w-full justify-between py-3"
      >
        <div className="text-left">
          {selected ? (
            <>
              <div className="font-semibold">
                {selected.title || selected.id}
              </div>
              <div className="text-xs text-muted-foreground">
                {selected.id}
                {selected.version ? ` · v${selected.version}` : ""}
              </div>
            </>
          ) : (
            <div className="text-muted-foreground">
              {isLoading ? "Loading processes…" : "Select a process..."}
            </div>
          )}
        </div>
        <ChevronDown className="ml-2 h-4 w-4" />
      </Button>

      {open && (
        <div className="absolute top-full z-50 mt-1 flex max-h-96 w-full flex-col overflow-hidden rounded-lg border bg-card shadow-xl">
          <div className="border-b p-2">
            <div className="relative">
              <Search className="absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
              <input
                type="text"
                value={search}
                onChange={(e) => setSearch(e.target.value)}
                placeholder="Search processes..."
                className="w-full rounded-md border bg-background py-2 pl-9 pr-3 text-sm focus:outline-none focus:ring-2 focus:ring-ring"
                autoFocus
              />
            </div>
          </div>
          <div className="overflow-y-auto">
            {isError ? (
              <div className="px-3 py-4 text-sm text-muted-foreground">
                Couldn&rsquo;t reach the API. Is Sepex running at{" "}
                <code className="font-mono">{API_URL}</code>?
              </div>
            ) : isLoading ? (
              <div className="space-y-2 p-2">
                <Skeleton className="h-10 w-full" />
                <Skeleton className="h-10 w-full" />
                <Skeleton className="h-10 w-full" />
              </div>
            ) : filtered.length === 0 ? (
              <div className="px-3 py-4 text-sm text-muted-foreground">
                No processes found.
              </div>
            ) : (
              filtered.map((p) => (
                <button
                  type="button"
                  key={p.id}
                  onClick={() => handleSelect(p.id)}
                  className="w-full border-b px-3 py-2.5 text-left transition-colors last:border-0 hover:bg-accent"
                >
                  <div className="font-medium">{p.title || p.id}</div>
                  <div className="text-xs text-muted-foreground">
                    {p.id}
                    {p.version ? ` · v${p.version}` : ""}
                  </div>
                </button>
              ))
            )}
          </div>
        </div>
      )}
    </div>
  );
}
