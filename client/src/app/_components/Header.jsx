import Link from "next/link";
import { Activity } from "lucide-react";
import CommandPaletteButton from "@/app/_components/CommandPaletteButton";
import MobileNav from "@/app/_components/MobileNav";
import { navItems } from "@/app/_components/navItems";
import NavLink from "@/app/_components/NavLink";
import UserPopover from "@/app/_components/UserPopover";
import { Separator } from "@/components/ui/separator";

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
            <NavLink key={item.href} href={item.href} label={item.label} />
          ))}
        </nav>
        <div className="ml-auto flex items-center gap-2">
          <CommandPaletteButton />
          <UserPopover />
        </div>
      </div>
      <MobileNav />
    </header>
  );
}
