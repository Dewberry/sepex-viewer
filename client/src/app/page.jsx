import Link from "next/link";
import { LayoutDashboard } from "lucide-react";
import { Button } from "@/components/ui/button";

export default function LandingPage() {
  return (
    <main className="flex min-h-screen flex-col items-center justify-center gap-6 px-6">
      <div className="flex flex-col items-center gap-3 text-center">
        <span className="text-xs font-medium uppercase tracking-[0.2em] text-cta">
          Dewberry
        </span>
        <h1 className="text-4xl font-semibold tracking-tight">Sepex Viewer</h1>
        <p className="max-w-md text-muted-foreground">
          Monitor jobs, compose payloads, and inspect results from the Sepex
          OGC API.
        </p>
      </div>
      <div className="flex gap-3">
        <Link href="/dashboard">
          <Button>
            <LayoutDashboard className="mr-1.5 h-4 w-4" />
            Open dashboard
          </Button>
        </Link>
        <Link href="/jobs">
          <Button variant="outline">Browse jobs</Button>
        </Link>
      </div>
    </main>
  );
}
