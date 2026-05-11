"use client";

import { useState } from "react";
import { ChevronDown } from "lucide-react";
import { Button } from "@/components/ui/button";
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList
} from "@/components/ui/command";
import {
  Popover,
  PopoverContent,
  PopoverTrigger
} from "@/components/ui/popover";
import { Skeleton } from "@/components/ui/skeleton";

const API_URL =
  process.env.NEXT_PUBLIC_API_URL ||
  process.env.NEXT_PUBLIC_SEPEX_BASE_URL ||
  "http://localhost:5050";

export default function ProcessPicker({
  processes,
  selected,
  isLoading,
  isError,
  onSelect
}) {
  const [open, setOpen] = useState(false);

  const handleSelect = (id) => {
    setOpen(false);
    onSelect(id);
  };

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <Button
          type="button"
          variant="outline"
          role="combobox"
          aria-label="Process"
          aria-expanded={open}
          aria-haspopup="listbox"
          className="h-auto w-full justify-between py-3"
        >
          <div className="text-left">
            {selected ? (
              <>
                <div className="font-semibold">
                  {selected.title || selected.id}
                </div>
                <div className="text-xs text-muted-foreground">
                  {selected.id}
                  {selected.version ? ` · v${selected.version}` : ""}
                </div>
              </>
            ) : (
              <div className="text-muted-foreground">
                {isLoading ? "Loading processes…" : "Select a process..."}
              </div>
            )}
          </div>
          <ChevronDown
            className="ml-2 h-4 w-4 shrink-0 opacity-70"
            aria-hidden="true"
          />
        </Button>
      </PopoverTrigger>
      <PopoverContent
        align="start"
        sideOffset={4}
        className="w-[var(--radix-popover-trigger-width)] p-0"
      >
        <Command
          filter={(value, search) => {
            if (!search) return 1;
            return value.toLowerCase().includes(search.toLowerCase()) ? 1 : 0;
          }}
        >
          <CommandInput placeholder="Search processes..." />
          <CommandList>
            {isError ? (
              <div className="px-3 py-4 text-sm text-muted-foreground">
                Couldn&rsquo;t reach the API. Is Sepex running at{" "}
                <code className="font-mono">{API_URL}</code>?
              </div>
            ) : isLoading ? (
              <div className="space-y-2 p-2">
                <Skeleton className="h-10 w-full" />
                <Skeleton className="h-10 w-full" />
                <Skeleton className="h-10 w-full" />
              </div>
            ) : (
              <>
                <CommandEmpty>No processes found.</CommandEmpty>
                <CommandGroup>
                  {processes.map((p) => {
                    const label = p.title || p.id;
                    return (
                      <CommandItem
                        key={p.id}
                        value={`${label} ${p.id}`}
                        onSelect={() => handleSelect(p.id)}
                        className="flex flex-col items-start gap-0.5 py-2"
                      >
                        <span className="font-medium">{label}</span>
                        <span className="text-xs text-muted-foreground">
                          {p.id}
                          {p.version ? ` · v${p.version}` : ""}
                        </span>
                      </CommandItem>
                    );
                  })}
                </CommandGroup>
              </>
            )}
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  );
}
