import type { Metadata } from "next";
import { Inter } from "next/font/google";
import "./globals.css";
import Sidebar from "../components/Sidebar";
import PasswordGate from "../components/PasswordGate";

const inter = Inter({ subsets: ["latin"] });

export const metadata: Metadata = {
  title: "Ellenor Funding",
  description: "Scrape and analyze charity funding opportunities",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en">
      <body className={`${inter.className} min-h-screen bg-slate-50`}>
        <PasswordGate>
          <div className="flex min-h-screen">
            <Sidebar />
            <main className="flex-1 overflow-y-auto bg-white p-8 shadow-inner">{children}</main>
          </div>
        </PasswordGate>
      </body>
    </html>
  );
}
