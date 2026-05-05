"use client";

import { TIME_RANGES } from "@/app/(dashboard)/dashboard/_utils/timeRange";

export default function TimeRangePicker({ value, onChange }) {
  return (
    <div
      role="tablist"
      aria-label="Time range"
      className="inline-flex items-center gap-0.5 rounded-lg bg-muted p-1"
    >
      {TIME_RANGES.map((range) => {
        const active = value === range;
        return (
          <button
            key={range}
            role="tab"
            aria-selected={active}
            onClick={() => onChange(range)}
            className={`rounded px-3 py-1.5 text-sm font-medium transition-colors ${
              active
                ? "bg-card text-foreground shadow-sm"
                : "text-muted-foreground hover:text-foreground"
            }`}
          >
            {range}
          </button>
        );
      })}
    </div>
  );
}
