"use client";

import { useRouter } from "next/navigation";
import { Search } from "lucide-react";
import { openCommandPalette } from "@/app/_components/CommandPalette";
import { Button } from "@/components/ui/button";
import { PROPOSED_API_ENABLED } from "@/lib/featureFlags";

export default function CommandPaletteButton() {
  const router = useRouter();

  const onClick = () => {
    if (PROPOSED_API_ENABLED) openCommandPalette();
    else router.push("/jobs?focus=search");
  };

  return (
    <Button
      variant="ghost"
      size="sm"
      className="hidden lg:flex gap-2 text-muted-foreground"
      onClick={onClick}
      aria-label="Search jobs"
    >
      <Search className="h-4 w-4" />
      <span className="text-xs">Search</span>
      <kbd className="pointer-events-none inline-flex h-5 select-none items-center gap-1 rounded border bg-muted px-1.5 font-mono text-[10px] font-medium text-muted-foreground">
        ⌘K
      </kbd>
    </Button>
  );
}
