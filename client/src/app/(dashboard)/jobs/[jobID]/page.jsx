export default async function JobDetailPage({ params }) {
  const { jobID } = await params;
  return (
    <section className="mx-auto max-w-7xl p-6">
      <h1 className="text-2xl font-semibold tracking-tight">
        Job <span className="font-mono">{jobID}</span>
      </h1>
      <p className="mt-2 text-muted-foreground">
        Job detail placeholder — status, logs, results, metadata.
      </p>
    </section>
  );
}
