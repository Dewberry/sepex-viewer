import Footer from "@/app/_components/Footer";
import GlobalShortcuts from "@/app/_components/GlobalShortcuts";
import Header from "@/app/_components/Header";

export default function DashboardLayout({ children }) {
  return (
    <div className="flex min-h-screen flex-col">
      <GlobalShortcuts />
      <Header />
      <main className="flex-1">{children}</main>
      <Footer />
    </div>
  );
}
