import { Info } from "lucide-react";

export default function PayloadPreview({ payloadJSON }) {
  return (
    <div className="rounded-lg border bg-card p-4">
      <div className="mb-3 flex items-center gap-2">
        <Info className="h-4 w-4 text-muted-foreground" aria-hidden="true" />
        <h2 className="text-sm font-semibold">Payload Preview</h2>
      </div>
      <pre className="max-h-64 overflow-auto rounded-md bg-muted p-3 font-mono text-xs">
        {payloadJSON}
      </pre>
    </div>
  );
}
