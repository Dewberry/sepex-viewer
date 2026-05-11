"use client";

import { useEffect, useState } from "react";
import { Monitor, Moon, Sun } from "lucide-react";
import { useTheme } from "next-themes";

const OPTIONS = [
  { id: "light", label: "Light mode", icon: Sun },
  { id: "dark", label: "Dark mode", icon: Moon },
  { id: "system", label: "System preference", icon: Monitor }
];

export default function ThemeSwitcher() {
  const { theme, setTheme } = useTheme();
  // Defer theme-dependent rendering until after hydration. The server can't
  // know the user's stored preference, so until we mount we treat every option
  // as inactive — matches the SSR output and avoids an aria-pressed mismatch.
  const [mounted, setMounted] = useState(false);
  useEffect(() => setMounted(true), []);

  return (
    <div className="flex items-center gap-2 text-xs text-muted-foreground">
      <span>Theme:</span>
      <div className="flex gap-1 rounded-lg bg-muted p-1">
        {OPTIONS.map(({ id, label, icon: Icon }) => {
          const active = mounted && theme === id;
          return (
            <button
              key={id}
              type="button"
              onClick={() => setTheme(id)}
              title={label}
              aria-label={label}
              aria-pressed={active}
              className={`rounded px-2 py-1 transition-colors ${
                active
                  ? "bg-background text-foreground shadow-sm"
                  : "text-muted-foreground hover:text-foreground"
              }`}
            >
              <Icon className="h-3.5 w-3.5" />
            </button>
          );
        })}
      </div>
    </div>
  );
}
