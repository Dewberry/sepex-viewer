import Link from "next/link";
import { ChevronLeft } from "lucide-react";
import JobDetailContent from "@/app/(dashboard)/jobs/[jobID]/_components/JobDetailContent";

export default async function JobDetailPage({ params }) {
  const { jobID } = await params;
  return (
    <div className="mx-auto max-w-[1200px] space-y-6 px-4 py-6 lg:px-6">
      <Link
        href="/jobs"
        className="inline-flex items-center gap-1 text-sm text-muted-foreground transition-colors hover:text-foreground"
      >
        <ChevronLeft className="h-4 w-4" />
        Back to jobs
      </Link>
      <JobDetailContent jobID={jobID} />
    </div>
  );
}
