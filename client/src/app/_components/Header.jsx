import Link from "next/link";
import { Activity } from "lucide-react";
import { Separator } from "@/components/ui/separator";

const navItems = [
  { href: "/dashboard", label: "Dashboard" },
  { href: "/builder", label: "Builder" },
  { href: "/jobs", label: "Jobs" }
];

export default function Header() {
  return (
    <header className="flex h-14 items-center gap-4 border-b border-border bg-background px-6">
      <Link href="/" className="flex items-center gap-2 font-semibold">
        <Activity className="h-5 w-5 text-primary" />
        <span>Sepex Viewer</span>
      </Link>
      <Separator orientation="vertical" className="h-6" />
      <nav className="flex items-center gap-1">
        {navItems.map((item) => (
          <Link
            key={item.href}
            href={item.href}
            className="rounded-md px-3 py-1.5 text-sm text-muted-foreground transition-colors hover:bg-accent hover:text-accent-foreground"
          >
            {item.label}
          </Link>
        ))}
      </nav>
    </header>
  );
}
