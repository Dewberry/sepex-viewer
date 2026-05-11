"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import { useQuery } from "@tanstack/react-query";
import {
  ArrowRight,
  LayoutDashboard,
  ListChecks,
  Plus,
  Search
} from "lucide-react";
import {
  CommandDialog,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
  CommandSeparator
} from "@/components/ui/command";
import { listJobs } from "@/lib/sepex";

const OPEN_EVENT = "sepex:open-command-palette";

export function openCommandPalette() {
  window.dispatchEvent(new Event(OPEN_EVENT));
}

export default function CommandPalette() {
  const router = useRouter();
  const [open, setOpen] = useState(false);
  const [query, setQuery] = useState("");

  useEffect(() => {
    const onKey = (e) => {
      if ((e.metaKey || e.ctrlKey) && e.key === "k") {
        e.preventDefault();
        setOpen((v) => !v);
      }
    };
    const onOpen = () => setOpen(true);
    window.addEventListener("keydown", onKey);
    window.addEventListener(OPEN_EVENT, onOpen);
    return () => {
      window.removeEventListener("keydown", onKey);
      window.removeEventListener(OPEN_EVENT, onOpen);
    };
  }, []);

  useEffect(() => {
    if (!open) setQuery("");
  }, [open]);

  const trimmed = query.trim();
  const jobsQuery = useQuery({
    queryKey: ["command-palette", "jobs", trimmed],
    queryFn: () => listJobs(trimmed ? { q: trimmed, limit: 8 } : { limit: 5 }),
    enabled: open,
    staleTime: 5_000
  });
  const jobs = jobsQuery.data?.jobs || [];

  const go = (path) => {
    setOpen(false);
    router.push(path);
  };

  return (
    <CommandDialog
      open={open}
      onOpenChange={setOpen}
      title="Search Sepex"
      description="Search jobs and jump to common actions."
    >
      <CommandInput
        value={query}
        onValueChange={setQuery}
        placeholder="Search jobs, submitters, processes…"
      />
      <CommandList>
        <CommandEmpty>
          {jobsQuery.isLoading ? "Searching…" : "No results."}
        </CommandEmpty>

        {jobs.length > 0 ? (
          <CommandGroup heading={trimmed ? "Jobs" : "Recent jobs"}>
            {jobs.map((job) => (
              <CommandItem
                key={job.jobID}
                value={`${job.jobID} ${job.submitter} ${job.processID}`}
                onSelect={() => go(`/jobs/${job.jobID}`)}
              >
                <Search />
                <span className="font-mono text-sm">{job.jobID}</span>
                <span className="ml-2 truncate text-xs text-muted-foreground">
                  {job.processID} · {job.submitter} · {job.status}
                </span>
                <ArrowRight className="ml-auto h-4 w-4" />
              </CommandItem>
            ))}
          </CommandGroup>
        ) : null}

        <CommandSeparator />

        <CommandGroup heading="Actions">
          <CommandItem value="submit new job" onSelect={() => go("/builder")}>
            <Plus />
            Submit new job
          </CommandItem>
          <CommandItem value="view dashboard" onSelect={() => go("/dashboard")}>
            <LayoutDashboard />
            View dashboard
          </CommandItem>
          <CommandItem value="view all jobs" onSelect={() => go("/jobs")}>
            <ListChecks />
            View all jobs
          </CommandItem>
        </CommandGroup>
      </CommandList>
    </CommandDialog>
  );
}
