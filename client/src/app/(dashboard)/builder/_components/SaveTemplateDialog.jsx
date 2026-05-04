"use client";

import { useState } from "react";
import { Button } from "@/components/ui/button";
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle
} from "@/components/ui/dialog";
import { Input } from "@/components/ui/input";

export default function SaveTemplateDialog({ open, onOpenChange, onSave }) {
  const [name, setName] = useState("");
  const [error, setError] = useState("");

  const handleOpenChange = (next) => {
    if (!next) {
      setName("");
      setError("");
    }
    onOpenChange(next);
  };

  const submit = () => {
    setError("");
    const trimmed = name.trim();
    if (!trimmed) {
      setError("Template name is required.");
      return;
    }
    try {
      onSave(trimmed);
      handleOpenChange(false);
    } catch (err) {
      setError(err?.message || "Could not save template.");
    }
  };

  return (
    <Dialog open={open} onOpenChange={handleOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Save Template</DialogTitle>
        </DialogHeader>
        <div className="space-y-2">
          <label className="block text-sm font-medium">Template name</label>
          <Input
            value={name}
            onChange={(e) => {
              setName(e.target.value);
              setError("");
            }}
            onKeyDown={(e) => {
              if (e.key === "Enter") {
                e.preventDefault();
                submit();
              }
            }}
            placeholder="e.g. Weekly Baseline"
            autoFocus
          />
          {error && <p className="text-xs text-status-failed">{error}</p>}
          <p className="text-xs italic text-muted-foreground">
            Saved in your browser. Other devices won&rsquo;t see these.
          </p>
        </div>
        <DialogFooter>
          <Button variant="outline" onClick={() => handleOpenChange(false)}>
            Cancel
          </Button>
          <Button onClick={submit}>Save</Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  );
}
