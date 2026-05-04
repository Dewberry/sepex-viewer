"use client";

import { useCallback } from "react";
import { usePathname, useRouter, useSearchParams } from "next/navigation";

export default function useSelectedJobUrlSync() {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const selected = searchParams.get("selected");

  const setSelected = useCallback(
    (jobID) => {
      const params = new URLSearchParams(Array.from(searchParams.entries()));
      if (jobID) params.set("selected", jobID);
      else params.delete("selected");
      const qs = params.toString();
      router.replace(qs ? `${pathname}?${qs}` : pathname, { scroll: false });
    },
    [pathname, router, searchParams]
  );

  return [selected, setSelected];
}
