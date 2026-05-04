import "@/app/styles/globals.css";
import { jetbrainsMono, openSans } from "@/app/styles/fonts";
import Providers from "@/app/providers";
import { TooltipProvider } from "@/components/ui/tooltip";

export const metadata = {
  title: "Sepex Viewer",
  description: "Job dashboard and payload builder for the Sepex API"
};

export default function RootLayout({ children }) {
  return (
    <html lang="en" suppressHydrationWarning>
      <body
        className={`${openSans.variable} ${jetbrainsMono.variable} font-sans antialiased`}
      >
        <Providers>
          <TooltipProvider>{children}</TooltipProvider>
        </Providers>
      </body>
    </html>
  );
}
