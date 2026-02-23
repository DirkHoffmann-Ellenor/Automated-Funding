"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";

const tabs = [
  { href: "/results", label: "Results" },
  { href: "/results/expired-scraped", label: "Expired and Incorrect Funds" },
];

export default function ResultsTabs() {
  const pathname = usePathname();

  return (
    <div className="flex flex-wrap items-center gap-2 border-b border-neutral-200 pb-3">
      {tabs.map((tab) => {
        const active = pathname === tab.href;
        return (
          <Link
            key={tab.href}
            href={tab.href}
            aria-current={active ? "page" : undefined}
            className={`rounded-full px-4 py-2 text-sm font-semibold transition ${
              active
                ? "bg-neutral-900 text-white shadow-sm"
                : "border border-neutral-200 bg-white text-neutral-700 hover:border-neutral-900"
            }`}
          >
            {tab.label}
          </Link>
        );
      })}
    </div>
  );
}
