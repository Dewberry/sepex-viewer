"use client";

import { useEffect } from "react";
import { useRouter } from "next/navigation";
import CommandPalette from "@/app/_components/CommandPalette";
import { PROPOSED_API_ENABLED } from "@/lib/featureFlags";

export default function GlobalShortcuts() {
  const router = useRouter();

  useEffect(() => {
    // When the proposed `?q=` substring search ships (item I), the global
    // ⌘K binding is owned by the CommandPalette below. Until then, ⌘K is
    // just a shortcut to the Jobs page search box.
    if (PROPOSED_API_ENABLED) return undefined;
    const handler = (e) => {
      if ((e.metaKey || e.ctrlKey) && e.key === "k") {
        e.preventDefault();
        router.push("/jobs?focus=search");
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [router]);

  return PROPOSED_API_ENABLED ? <CommandPalette /> : null;
}
