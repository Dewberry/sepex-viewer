"use client";

const MODES = [
  { id: "async", label: "Async", help: "Returns immediately with job ID" },
  {
    id: "sync",
    label: "Sync",
    help: "Waits for completion and returns outputs"
  }
];

export default function ExecutionModeCard({ mode, onChange }) {
  return (
    <div className="rounded-lg border bg-card p-4">
      <label className="mb-3 block text-sm font-medium">Execution Mode</label>
      <div className="space-y-2">
        {MODES.map((m) => (
          <label key={m.id} className="flex cursor-pointer items-center gap-3">
            <input
              type="radio"
              name="execution-mode"
              value={m.id}
              checked={mode === m.id}
              onChange={() => onChange(m.id)}
              className="h-4 w-4 border-input text-dewberry-teal focus:ring-2 focus:ring-ring"
            />
            <div>
              <div className="text-sm font-medium">{m.label}</div>
              <div className="text-xs text-muted-foreground">{m.help}</div>
            </div>
          </label>
        ))}
      </div>
    </div>
  );
}
