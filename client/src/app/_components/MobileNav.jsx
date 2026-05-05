import Link from "next/link";
import { navItems } from "@/app/_components/navItems";

export default function MobileNav() {
  return (
    <div className="border-t border-border px-4 py-2 md:hidden">
      <nav className="flex items-center gap-1 text-sm">
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
    </div>
  );
}
