"use client";

import { Search } from "lucide-react";
import { Input } from "@/components/ui/input";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue
} from "@/components/ui/select";
import { JOB_STATUSES } from "@/lib/sepex";

const ANY_VALUE = "__any__";

export default function JobsFilterBar({
  filters,
  onChange,
  processes,
  processesLoading,
  searchInputRef
}) {
  const update = (patch) => onChange({ ...filters, ...patch });

  return (
    <div className="rounded-lg border border-border bg-card p-4">
      <div className="grid gap-3 md:grid-cols-2 lg:grid-cols-4">
        <div className="relative">
          <Search className="absolute top-1/2 left-3 h-4 w-4 -translate-y-1/2 text-muted-foreground" />
          <Input
            ref={searchInputRef}
            type="text"
            placeholder="Search jobID or submitter…"
            value={filters.search}
            onChange={(e) => update({ search: e.target.value })}
            className="pl-9"
          />
        </div>

        <Select
          value={filters.processID || ANY_VALUE}
          onValueChange={(v) => update({ processID: v === ANY_VALUE ? "" : v })}
        >
          <SelectTrigger className="w-full" aria-label="Filter by process">
            <SelectValue
              placeholder={processesLoading ? "Loading…" : "Any process"}
            />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value={ANY_VALUE}>Any process</SelectItem>
            {processes.map((p) => (
              <SelectItem key={p.id} value={p.id}>
                {p.title || p.id}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>

        <Select
          value={filters.status || ANY_VALUE}
          onValueChange={(v) => update({ status: v === ANY_VALUE ? "" : v })}
        >
          <SelectTrigger className="w-full" aria-label="Filter by status">
            <SelectValue placeholder="Any status" />
          </SelectTrigger>
          <SelectContent>
            <SelectItem value={ANY_VALUE}>Any status</SelectItem>
            {JOB_STATUSES.map((s) => (
              <SelectItem key={s} value={s}>
                {s}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>

        <Input
          type="text"
          placeholder="Submitter…"
          value={filters.submitter}
          onChange={(e) => update({ submitter: e.target.value })}
        />
      </div>
    </div>
  );
}
