"use client";

import { Input } from "@/components/ui/input";

export default function SubmitterCard({ value, onChange }) {
  return (
    <div className="rounded-lg border bg-card p-4">
      <label
        htmlFor="submitter-email"
        className="mb-2 block text-sm font-medium"
      >
        Submitter Email
      </label>
      <Input
        id="submitter-email"
        type="email"
        autoComplete="email"
        value={value}
        onChange={(e) => onChange(e.target.value)}
        placeholder="user@example.com"
      />
    </div>
  );
}
