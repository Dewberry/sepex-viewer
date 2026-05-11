"use client";

import { ChevronLeft, ChevronRight } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue
} from "@/components/ui/select";

const PAGE_SIZES = [10, 20, 50, 100];

export default function PaginationFooter({
  page,
  totalPages,
  pageSize,
  onPageSize,
  hasPrev,
  hasNext,
  onPrev,
  onNext
}) {
  return (
    <div className="flex flex-wrap items-center justify-between gap-2 border-t border-border px-4 py-3 text-sm">
      <div className="flex items-center gap-2 text-muted-foreground">
        <span>Per page</span>
        <Select
          value={String(pageSize)}
          onValueChange={(v) => onPageSize(Number(v))}
        >
          <SelectTrigger size="sm" className="w-20" aria-label="Items per page">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {PAGE_SIZES.map((n) => (
              <SelectItem key={n} value={String(n)}>
                {n}
              </SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>
      <div className="flex items-center gap-2">
        <Button
          variant="ghost"
          size="sm"
          onClick={onPrev}
          disabled={!hasPrev}
          className="gap-2"
        >
          <ChevronLeft className="h-4 w-4" />
          Prev
        </Button>
        <span className="text-muted-foreground">
          Page {page}
          {totalPages ? ` of ${totalPages}` : ""}
        </span>
        <Button
          variant="ghost"
          size="sm"
          onClick={onNext}
          disabled={!hasNext}
          className="gap-2"
        >
          Next
          <ChevronRight className="h-4 w-4" />
        </Button>
      </div>
    </div>
  );
}
