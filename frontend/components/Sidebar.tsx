"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useState } from "react";

const navItems = [
  { href: "/", label: "Scrape & Analyze", emoji: "🌐" },
  { href: "/results", label: "Results", emoji: "📊" },
  { href: "/settings", label: "Settings", emoji: "⚙️" },
];

export default function Sidebar() {
  const pathname = usePathname();
  const [collapsed, setCollapsed] = useState(false);

  return (
    <aside className={`flex flex-col bg-slate-900 text-white transition-all ${collapsed ? "w-18" : "w-64"}`}>
      <div className="flex items-center justify-between px-4 py-5">
        <div>
          <p className="text-lg font-semibold">ellenor Funding</p>
          <p className="text-sm text-slate-300">Scrape → Analyze → Review</p>
        </div>
        <button
          className="rounded-full bg-slate-800 p-2 text-xs"
          onClick={() => setCollapsed((v) => !v)}
          aria-label="Toggle navigation"
        >
          {collapsed ? ">" : "<"}
        </button>
      </div>
      <nav className="flex-1 space-y-2 px-3">
        {navItems.map((item) => {
          const active = pathname === item.href;
          return (
            <Link key={item.href} href={item.href} className={`sidebar-link ${active ? "bg-white/10" : "text-slate-200"}`}>
              <span>{item.emoji}</span>
              {!collapsed && <span>{item.label}</span>}
            </Link>
          );
        })}
      </nav>
      <div className="px-4 py-4 text-xs text-slate-400">
        API base: {process.env.NEXT_PUBLIC_API_BASE_URL || "http://localhost:8000"}
      </div>
    </aside>
  );
}
