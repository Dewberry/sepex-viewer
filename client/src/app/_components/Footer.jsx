import ThemeSwitcher from "@/app/_components/ThemeSwitcher";

export default function Footer() {
  return (
    <footer className="border-t border-border bg-card px-4 py-3 lg:px-6">
      <div className="mx-auto flex max-w-[1800px] flex-col items-center justify-between gap-2 text-xs text-muted-foreground sm:flex-row">
        <div>Sepex Viewer · Dewberry Resilience Solutions</div>
        <ThemeSwitcher />
      </div>
    </footer>
  );
}
