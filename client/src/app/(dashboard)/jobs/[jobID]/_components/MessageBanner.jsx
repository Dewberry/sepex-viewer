import { Info } from "lucide-react";

export default function MessageBanner({ message }) {
  if (!message) return null;
  return (
    <div className="flex items-center gap-2 rounded-md border border-border bg-accent/50 px-3 py-2 text-sm">
      <Info className="h-4 w-4 shrink-0 text-muted-foreground" />
      <span className="text-muted-foreground">Last update:</span>
      <span>{message}</span>
    </div>
  );
}
