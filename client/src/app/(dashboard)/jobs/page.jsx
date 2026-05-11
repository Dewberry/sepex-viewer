"use client";

import { Suspense, useEffect, useMemo, useRef, useState } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { useQuery, useQueryClient } from "@tanstack/react-query";
import AppliedFiltersBar from "@/app/(dashboard)/jobs/_components/AppliedFiltersBar";
import BulkDismissBar from "@/app/(dashboard)/jobs/_components/BulkDismissBar";
import JobDetailDrawer from "@/app/(dashboard)/jobs/_components/JobDetailDrawer";
import JobsFilterBar from "@/app/(dashboard)/jobs/_components/JobsFilterBar";
import JobsPageHeader from "@/app/(dashboard)/jobs/_components/JobsPageHeader";
import JobsTable from "@/app/(dashboard)/jobs/_components/JobsTable";
import PaginationFooter from "@/app/(dashboard)/jobs/_components/PaginationFooter";
import useBulkDismiss from "@/app/(dashboard)/jobs/_hooks/useBulkDismiss";
import useJobsQuery from "@/app/(dashboard)/jobs/_hooks/useJobsQuery";
import useSelectedJobUrlSync from "@/app/(dashboard)/jobs/_hooks/useSelectedJobUrlSync";
import buildJobsQueryParams from "@/app/(dashboard)/jobs/_utils/buildJobsQueryParams";
import { ACTIVE_STATUSES, listProcesses } from "@/lib/sepex";

const FILTER_KEYS = ["search", "processID", "status", "submitter", "tags"];
const EMPTY_FILTERS = Object.fromEntries(FILTER_KEYS.map((k) => [k, ""]));

const filtersFromSearchParams = (sp) =>
  Object.fromEntries(FILTER_KEYS.map((k) => [k, sp?.get(k) ?? ""]));

function JobsPageInner() {
  const queryClient = useQueryClient();
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const searchInputRef = useRef(null);
  const [selectedJobID, setSelectedJobID] = useSelectedJobUrlSync();

  const [filters, setFilters] = useState(() =>
    filtersFromSearchParams(searchParams)
  );
  const [pageSize, setPageSize] = useState(20);
  const [offset, setOffset] = useState(0);

  // Auto-focus search when arriving via ⌘K (?focus=search), then strip the param.
  useEffect(() => {
    if (searchParams.get("focus") !== "search") return;
    searchInputRef.current?.focus();
    const next = new URLSearchParams(searchParams);
    next.delete("focus");
    const qs = next.toString();
    router.replace(qs ? `${pathname}?${qs}` : pathname, { scroll: false });
  }, [searchParams, router, pathname]);

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
    () => buildJobsQueryParams(filters, pageSize, offset),
    [filters, pageSize, offset]
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

  const bulk = useBulkDismiss(visibleJobs);

  const hasActiveJobs = visibleJobs.some((j) => ACTIVE_STATUSES.has(j.status));
  const hasActiveFilters = Object.entries(filters).some(([, v]) =>
    typeof v === "string" ? v.trim() !== "" : Boolean(v)
  );
  const hasNext = links.some((l) => l.title === "next" || l.rel === "next");
  const hasPrev = offset > 0;
  const page = Math.floor(offset / pageSize) + 1;

  const handleClearAll = () => setFilters(EMPTY_FILTERS);
  const handleRefresh = () =>
    queryClient.invalidateQueries({ queryKey: ["jobs"] });

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
          searchInputRef={searchInputRef}
        />
        <AppliedFiltersBar
          filters={filters}
          onChange={setFilters}
          onClearAll={handleClearAll}
        />
      </div>

      <div className="overflow-hidden rounded-lg border border-border bg-card">
        <BulkDismissBar
          selectedCount={bulk.selectedIDs.length}
          isPending={bulk.isPending}
          onDismiss={bulk.dismiss}
          onClear={bulk.clear}
        />
        <JobsTable
          jobs={visibleJobs}
          isLoading={jobsQuery.isLoading}
          isError={jobsQuery.isError}
          error={jobsQuery.error}
          hasActiveFilters={hasActiveFilters}
          selectedJobID={selectedJobID}
          onOpenJob={setSelectedJobID}
          selectedIDs={bulk.selectedIDs}
          onToggleRow={bulk.toggleRow}
          onToggleAll={bulk.toggleAll}
        />
        <PaginationFooter
          page={page}
          totalPages={jobsQuery.data?.pages || null}
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
        open={Boolean(selectedJobID)}
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
