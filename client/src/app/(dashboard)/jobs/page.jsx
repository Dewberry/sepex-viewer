"use client";

import { Suspense, useEffect, useMemo, useState } from "react";
import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { toast } from "sonner";
import AppliedFiltersBar from "@/app/(dashboard)/jobs/_components/AppliedFiltersBar";
import BulkDismissBar from "@/app/(dashboard)/jobs/_components/BulkDismissBar";
import JobDetailDrawer from "@/app/(dashboard)/jobs/_components/JobDetailDrawer";
import JobsFilterBar from "@/app/(dashboard)/jobs/_components/JobsFilterBar";
import JobsPageHeader from "@/app/(dashboard)/jobs/_components/JobsPageHeader";
import JobsTable from "@/app/(dashboard)/jobs/_components/JobsTable";
import PaginationFooter from "@/app/(dashboard)/jobs/_components/PaginationFooter";
import useJobsQuery from "@/app/(dashboard)/jobs/_hooks/useJobsQuery";
import useSelectedJobUrlSync from "@/app/(dashboard)/jobs/_hooks/useSelectedJobUrlSync";
import { ACTIVE_STATUSES, dismissJob, listProcesses } from "@/lib/sepex";

const EMPTY_FILTERS = {
  search: "",
  processID: "",
  status: "",
  submitter: "",
  tags: ""
};

function JobsPageInner() {
  const queryClient = useQueryClient();
  const [selectedJobID, setSelectedJobID] = useSelectedJobUrlSync();

  const [filters, setFilters] = useState(EMPTY_FILTERS);
  const [pageSize, setPageSize] = useState(20);
  const [offset, setOffset] = useState(0);
  const [selectedIDs, setSelectedIDs] = useState([]);

  // Reset to first page whenever a server-side filter or page size changes.
  useEffect(() => {
    setOffset(0);
  }, [
    filters.processID,
    filters.status,
    filters.submitter,
    filters.tags,
    pageSize
  ]);

  const queryParams = useMemo(
    () => ({
      limit: pageSize,
      offset,
      processID: filters.processID || undefined,
      status: filters.status || undefined,
      submitter: filters.submitter || undefined,
      tags: filters.tags
        ? filters.tags
            .split(",")
            .map((t) => t.trim())
            .filter(Boolean)
            .join(",")
        : undefined
    }),
    [
      pageSize,
      offset,
      filters.processID,
      filters.status,
      filters.submitter,
      filters.tags
    ]
  );

  const jobsQuery = useJobsQuery(queryParams);
  const links = jobsQuery.data?.links || [];

  const processesQuery = useQuery({
    queryKey: ["processes"],
    queryFn: () => listProcesses()
  });
  const processes = processesQuery.data?.processes || [];

  // Client-side search across jobID + submitter (the API has no ?q= yet).
  const visibleJobs = useMemo(() => {
    const jobs = jobsQuery.data?.jobs || [];
    const q = filters.search.trim().toLowerCase();
    if (!q) return jobs;
    return jobs.filter(
      (j) =>
        (j.jobID || "").toLowerCase().includes(q) ||
        (j.submitter || "").toLowerCase().includes(q)
    );
  }, [jobsQuery.data, filters.search]);

  const hasActiveJobs = visibleJobs.some((j) => ACTIVE_STATUSES.has(j.status));

  const hasNext = links.some((l) => l.title === "next" || l.rel === "next");
  const hasPrev = offset > 0;
  const page = Math.floor(offset / pageSize) + 1;

  const handleClearAll = () => setFilters(EMPTY_FILTERS);
  const handleRefresh = () =>
    queryClient.invalidateQueries({ queryKey: ["jobs"] });

  const toggleRow = (jobID, checked) => {
    setSelectedIDs((prev) =>
      checked ? [...prev, jobID] : prev.filter((id) => id !== jobID)
    );
  };

  const toggleAll = (checked) => {
    if (!checked) {
      setSelectedIDs([]);
      return;
    }
    setSelectedIDs(
      visibleJobs
        .filter((j) => ACTIVE_STATUSES.has(j.status))
        .map((j) => j.jobID)
    );
  };

  const dismissMutation = useMutation({
    mutationFn: async (ids) => {
      const results = await Promise.allSettled(ids.map((id) => dismissJob(id)));
      return results.map((r, i) => ({ id: ids[i], result: r }));
    },
    onSuccess: (results) => {
      const ok = results.filter((r) => r.result.status === "fulfilled").length;
      const fail = results.length - ok;
      if (ok > 0) toast.success(`Dismissed ${ok} job${ok === 1 ? "" : "s"}`);
      if (fail > 0)
        toast.error(`Failed to dismiss ${fail} job${fail === 1 ? "" : "s"}`);
      setSelectedIDs([]);
      queryClient.invalidateQueries({ queryKey: ["jobs"] });
    },
    onError: (err) => toast.error(err?.message || "Dismiss failed")
  });

  const handleDismiss = () => {
    if (selectedIDs.length === 0) return;
    dismissMutation.mutate(selectedIDs);
  };

  const drawerOpen = Boolean(selectedJobID);

  return (
    <div className="mx-auto max-w-[1800px] space-y-6 px-4 py-6 lg:px-6">
      <JobsPageHeader
        resultCount={visibleJobs.length}
        hasActiveJobs={hasActiveJobs}
        dataUpdatedAt={jobsQuery.dataUpdatedAt}
        isFetching={jobsQuery.isFetching}
        onRefresh={handleRefresh}
      />

      <div className="space-y-3">
        <JobsFilterBar
          filters={filters}
          onChange={setFilters}
          processes={processes}
          processesLoading={processesQuery.isLoading}
        />
        <AppliedFiltersBar
          filters={filters}
          onChange={setFilters}
          onClearAll={handleClearAll}
        />
      </div>

      <div className="overflow-hidden rounded-lg border border-border bg-card">
        <BulkDismissBar
          selectedCount={selectedIDs.length}
          isPending={dismissMutation.isPending}
          onDismiss={handleDismiss}
          onClear={() => setSelectedIDs([])}
        />
        <JobsTable
          jobs={visibleJobs}
          isLoading={jobsQuery.isLoading}
          isError={jobsQuery.isError}
          error={jobsQuery.error}
          selectedJobID={selectedJobID}
          onOpenJob={(id) => setSelectedJobID(id)}
          selectedIDs={selectedIDs}
          onToggleRow={toggleRow}
          onToggleAll={toggleAll}
        />
        <PaginationFooter
          page={page}
          totalPages={null}
          pageSize={pageSize}
          onPageSize={setPageSize}
          hasPrev={hasPrev}
          hasNext={hasNext}
          onPrev={() => setOffset(Math.max(0, offset - pageSize))}
          onNext={() => setOffset(offset + pageSize)}
        />
      </div>

      <JobDetailDrawer
        jobID={selectedJobID}
        open={drawerOpen}
        onOpenChange={(open) => {
          if (!open) setSelectedJobID(null);
        }}
      />
    </div>
  );
}

export default function JobsPage() {
  // useSearchParams() needs a Suspense boundary in App Router during SSG.
  return (
    <Suspense fallback={null}>
      <JobsPageInner />
    </Suspense>
  );
}
