import Link from "next/link";
import { Activity } from "lucide-react";
import CommandPaletteButton from "./CommandPaletteButton";
import MobileNav from "./MobileNav";
import ThemeToggle from "./ThemeToggle";
import UserPopover from "./UserPopover";
import { Separator } from "@/components/ui/separator";

const navItems = [
  { href: "/dashboard", label: "Dashboard" },
  { href: "/builder", label: "Builder" },
  { href: "/jobs", label: "Jobs" }
];

export default function Header() {
  return (
    <header className="sticky top-0 z-40 border-b border-border bg-card/95 backdrop-blur supports-[backdrop-filter]:bg-card/80">
      <div className="flex h-14 items-center gap-4 px-4 lg:px-6">
        <Link href="/" className="flex items-center gap-2 font-semibold">
          <Activity className="h-5 w-5 text-primary" />
          <span>Sepex Viewer</span>
        </Link>
        <Separator orientation="vertical" className="hidden h-6 md:block" />
        <nav className="hidden items-center gap-1 md:flex">
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
        <div className="ml-auto flex items-center gap-2">
          <CommandPaletteButton />
          <ThemeToggle />
          <UserPopover />
        </div>
      </div>
      <MobileNav />
    </header>
  );
}
