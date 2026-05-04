export default function ProcessInfoCard({ process: p }) {
  if (!p) return null;
  return (
    <div className="rounded-lg border bg-card p-4">
      <div className="mb-2 flex items-start justify-between gap-4">
        <div>
          <h3 className="font-semibold">{p.title || p.id}</h3>
          {p.description ? (
            <p className="mt-1 text-sm text-muted-foreground">{p.description}</p>
          ) : null}
        </div>
        {p.version ? (
          <div className="font-mono text-xs text-muted-foreground">
            v{p.version}
          </div>
        ) : null}
      </div>
      {(p.jobControlOptions || []).length > 0 && (
        <div className="mt-3 flex flex-wrap gap-2">
          {p.jobControlOptions.map((opt) => (
            <span
              key={opt}
              className="rounded-md bg-dewberry-teal/10 px-2 py-1 text-xs font-medium text-dewberry-teal"
            >
              {opt}
            </span>
          ))}
        </div>
      )}
    </div>
  );
}
