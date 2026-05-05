"use client";

import { useMemo, useState } from "react";
import { useQueryClient } from "@tanstack/react-query";
import ComputeResourcesCard from "@/app/(dashboard)/dashboard/_components/ComputeResourcesCard";
import DashboardPageHeader from "@/app/(dashboard)/dashboard/_components/DashboardPageHeader";
import FailedJobsAlert from "@/app/(dashboard)/dashboard/_components/FailedJobsAlert";
import JobsByProcessChart from "@/app/(dashboard)/dashboard/_components/JobsByProcessChart";
import JobsOverTimeChart from "@/app/(dashboard)/dashboard/_components/JobsOverTimeChart";
import KpiTiles from "@/app/(dashboard)/dashboard/_components/KpiTiles";
import RecentActivityFeed from "@/app/(dashboard)/dashboard/_components/RecentActivityFeed";
import TopSubmittersChart from "@/app/(dashboard)/dashboard/_components/TopSubmittersChart";
import useComputeResources from "@/app/(dashboard)/dashboard/_hooks/useComputeResources";
import useDashboardJobs from "@/app/(dashboard)/dashboard/_hooks/useDashboardJobs";
import useFailedJobs from "@/app/(dashboard)/dashboard/_hooks/useFailedJobs";
import aggregateByProcess from "@/app/(dashboard)/dashboard/_utils/aggregateByProcess";
import aggregateBySubmitter from "@/app/(dashboard)/dashboard/_utils/aggregateBySubmitter";
import bucketJobsOverTime from "@/app/(dashboard)/dashboard/_utils/bucketJobsOverTime";
import computeKpis from "@/app/(dashboard)/dashboard/_utils/computeKpis";
import { getRangeStart } from "@/app/(dashboard)/dashboard/_utils/timeRange";

export default function DashboardPage() {
  const [range, setRange] = useState("24h");
  const queryClient = useQueryClient();

  const jobsQuery = useDashboardJobs(range);
  const resourcesQuery = useComputeResources();
  const failedQuery = useFailedJobs(5);

  const allJobs = useMemo(() => jobsQuery.data?.jobs || [], [jobsQuery.data]);

  // Client-side window: until the API ships ?updatedAfter / ?updatedBefore,
  // we filter the most-recent fetch down to the selected range.
  const windowedJobs = useMemo(() => {
    const start = getRangeStart(range).getTime();
    return allJobs.filter((j) => {
      if (!j.updated) return false;
      const t = new Date(j.updated).getTime();
      return !Number.isNaN(t) && t >= start;
    });
  }, [allJobs, range]);

  const kpis = useMemo(() => computeKpis(windowedJobs), [windowedJobs]);
  const overTime = useMemo(
    () => bucketJobsOverTime(windowedJobs, range),
    [windowedJobs, range]
  );
  const byProcess = useMemo(
    () => aggregateByProcess(windowedJobs),
    [windowedJobs]
  );
  const bySubmitter = useMemo(
    () => aggregateBySubmitter(windowedJobs),
    [windowedJobs]
  );

  const handleRefresh = () => {
    queryClient.invalidateQueries({ queryKey: ["dashboard"] });
    queryClient.invalidateQueries({ queryKey: ["admin", "resources"] });
    queryClient.invalidateQueries({ queryKey: ["job-logs"] });
  };

  const headerFetching =
    jobsQuery.isFetching || resourcesQuery.isFetching || failedQuery.isFetching;

  return (
    <div className="mx-auto max-w-[1600px] space-y-6 px-4 py-6 lg:px-6">
      <DashboardPageHeader
        range={range}
        onRangeChange={setRange}
        isFetching={headerFetching}
        onRefresh={handleRefresh}
      />

      <KpiTiles kpis={kpis} isLoading={jobsQuery.isLoading} />

      <div className="grid gap-4 lg:grid-cols-2">
        <JobsOverTimeChart
          data={overTime}
          isLoading={jobsQuery.isLoading}
          isError={jobsQuery.isError}
        />
        <JobsByProcessChart
          data={byProcess}
          isLoading={jobsQuery.isLoading}
          isError={jobsQuery.isError}
        />
      </div>

      <div className="grid gap-4 lg:grid-cols-2">
        <ComputeResourcesCard
          data={resourcesQuery.data}
          isLoading={resourcesQuery.isLoading}
          isError={resourcesQuery.isError}
          dataUpdatedAt={resourcesQuery.dataUpdatedAt}
        />
        <TopSubmittersChart
          data={bySubmitter}
          isLoading={jobsQuery.isLoading}
          isError={jobsQuery.isError}
        />
      </div>

      <FailedJobsAlert
        jobs={failedQuery.data?.jobs}
        isLoading={failedQuery.isLoading}
        isError={failedQuery.isError}
      />

      <RecentActivityFeed
        jobs={allJobs}
        isLoading={jobsQuery.isLoading}
        isError={jobsQuery.isError}
        limit={8}
      />
    </div>
  );
}
