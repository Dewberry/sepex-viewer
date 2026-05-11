import { navItems } from "@/app/_components/navItems";
import NavLink from "@/app/_components/NavLink";

export default function MobileNav() {
  return (
    <div className="border-t border-border px-4 py-2 md:hidden">
      <nav className="flex items-center gap-1 text-sm">
        {navItems.map((item) => (
          <NavLink key={item.href} href={item.href} label={item.label} />
        ))}
      </nav>
    </div>
  );
}
