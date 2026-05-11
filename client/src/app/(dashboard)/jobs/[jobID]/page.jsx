import JobBreadcrumb from "@/app/(dashboard)/jobs/[jobID]/_components/JobBreadcrumb";
import JobDetailContent from "@/app/(dashboard)/jobs/[jobID]/_components/JobDetailContent";

export async function generateMetadata({ params }) {
  const { jobID } = await params;
  return { title: `Job ${jobID}` };
}

export default async function JobDetailPage({ params }) {
  const { jobID } = await params;
  return (
    <div className="mx-auto max-w-[1200px] space-y-6 px-4 py-6 lg:px-6">
      <JobBreadcrumb jobID={jobID} />
      <JobDetailContent jobID={jobID} />
    </div>
  );
}
