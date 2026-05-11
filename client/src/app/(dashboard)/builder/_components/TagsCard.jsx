"use client";

import { useState } from "react";
import { Plus, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";

export default function TagsCard({ tags, onAdd, onRemove }) {
  const [draft, setDraft] = useState("");

  const submitTag = () => {
    const t = draft.trim();
    if (t) onAdd(t);
    setDraft("");
  };

  return (
    <div className="rounded-lg border bg-card p-4">
      <label className="mb-2 block text-sm font-medium">Tags</label>
      {tags.length > 0 && (
        <div className="mb-2 flex flex-wrap gap-2">
          {tags.map((tag) => (
            <span
              key={tag}
              className="inline-flex items-center gap-1 rounded-md bg-dewberry-teal/10 px-2 py-1 text-xs font-medium text-dewberry-teal"
            >
              {tag}
              <button
                type="button"
                onClick={() => onRemove(tag)}
                className="rounded-sm p-0.5 hover:bg-dewberry-teal/20"
                aria-label={`Remove ${tag}`}
              >
                <X className="h-3 w-3" />
              </button>
            </span>
          ))}
        </div>
      )}
      <div className="flex gap-2">
        <Input
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") {
              e.preventDefault();
              submitTag();
            }
          }}
          placeholder="Add tag…"
        />
        <Button size="icon" onClick={submitTag} aria-label="Add tag">
          <Plus className="h-3 w-3" />
        </Button>
      </div>
    </div>
  );
}
