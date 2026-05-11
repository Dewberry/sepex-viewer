"use client";

import { useState } from "react";
import { Trash2 } from "lucide-react";
import useDismissJob from "@/app/(dashboard)/jobs/[jobID]/_hooks/useDismissJob";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle
} from "@/components/ui/dialog";

export default function DismissJobButton({ jobID }) {
  const [open, setOpen] = useState(false);
  const dismiss = useDismissJob(jobID);

  return (
    <>
      <Button
        variant="outline"
        size="sm"
        onClick={() => setOpen(true)}
        disabled={dismiss.isPending}
        className="gap-2 text-destructive-fg hover:text-destructive-fg"
      >
        <Trash2 className="h-4 w-4" />
        <span className="hidden sm:inline">Dismiss</span>
      </Button>
      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Dismiss this job?</DialogTitle>
            <DialogDescription>
              Cancels the job on the server. The job will move to the dismissed
              state and can&rsquo;t be resumed.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter>
            <Button variant="outline" onClick={() => setOpen(false)}>
              Cancel
            </Button>
            <Button
              variant="destructive"
              onClick={() =>
                dismiss.mutate(undefined, { onSettled: () => setOpen(false) })
              }
              disabled={dismiss.isPending}
            >
              {dismiss.isPending ? "Dismissing…" : "Dismiss job"}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  );
}
