"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useState } from "react";

const navItems = [
  { href: "/", label: "Scrape & Analyze", emoji: "S" },
  { href: "/results", label: "Results", emoji: "R" },
  { href: "/settings", label: "Settings", emoji: "Cfg" },
];

export default function Sidebar() {
  const pathname = usePathname();
  const [collapsed, setCollapsed] = useState(false);

  return (
    <aside
      className={`relative flex flex-col overflow-hidden bg-gradient-to-b from-slate-900 via-slate-950 to-slate-900 text-white shadow-xl transition-all ${
        collapsed ? "w-16" : "w-72"
      }`}
    >
      <div className="absolute inset-x-0 top-0 h-32 bg-gradient-to-b from-brand/30 to-transparent blur-3xl opacity-60" />

      <div className="flex items-center justify-between px-4 py-5">
        <div className="flex items-center gap-3">
          <div className="flex h-10 w-10 items-center justify-center rounded-2xl bg-white/10 text-lg font-bold shadow-inner">
            eF
          </div>
          {!collapsed && (
            <div>
              <p className="text-lg font-semibold">ellenor Funding</p>
              <p className="text-xs uppercase tracking-wide text-slate-300">Scrape | Analyze | Review</p>
            </div>
          )}
        </div>
        <button
          className="flex h-9 w-9 items-center justify-center rounded-full border border-white/10 bg-white/5 text-sm font-semibold hover:bg-white/10 focus:outline-none focus:ring-2 focus:ring-brand"
          onClick={() => setCollapsed((v) => !v)}
          aria-label="Toggle navigation"
        >
          {collapsed ? ">" : "<"}
        </button>
      </div>

      <nav className="flex-1 space-y-2 px-3 pb-4">
        {navItems.map((item) => {
          const active = pathname === item.href;
          return (
            <Link
              key={item.href}
              href={item.href}
              className={`sidebar-link ${collapsed ? "justify-center" : ""} ${
                active ? "bg-white/15 text-white ring-1 ring-inset ring-white/20" : "text-slate-200"
              }`}
            >
              <span className="text-lg">{item.emoji}</span>
              {!collapsed && <span>{item.label}</span>}
            </Link>
          );
        })}
      </nav>

      <div className="m-3 rounded-xl border border-white/10 bg-white/5 px-4 py-3 text-xs text-slate-300 backdrop-blur">
        <p className="text-[11px] uppercase tracking-wide text-slate-400">API base</p>
        <p className="truncate text-sm font-semibold text-white"></p>
      </div>
    </aside>
  );
}
