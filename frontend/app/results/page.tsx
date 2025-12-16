"use client";

import type { ReactNode } from "react";
import { useEffect, useMemo, useState } from "react";
import { api } from "../../lib/api";

type ResultRecord = Record<string, string>;

export default function ResultsPage() {
  const [data, setData] = useState<ResultRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [eligibilityFilter, setEligibilityFilter] = useState<string[]>([]);
  const [search, setSearch] = useState("");

  useEffect(() => {
    const load = async () => {
      setLoading(true);
      try {
        const res = await api.results();
        setData(res.results || []);
        setEligibilityFilter(["Highly Eligible", "Eligible", "Possibly Eligible", "Low Match", "Not Eligible"]);
      } catch (err: any) {
        setError(err.message);
      } finally {
        setLoading(false);
      }
    };
    load();
  }, []);

  const filtered = useMemo(() => {
    return data.filter((row) => {
      const elig = row.eligibility || "";
      const inFilter = eligibilityFilter.length === 0 || eligibilityFilter.includes(elig);
      const query = search.toLowerCase();
      const matchesQuery =
        !query ||
        Object.values(row).some((val) => (val || "").toLowerCase().includes(query));
      return inFilter && matchesQuery;
    });
  }, [data, eligibilityFilter, search]);

  return (
    <div className="space-y-6">
      <header>
        <p className="text-sm uppercase tracking-wide text-slate-500">Results</p>
        <h1 className="text-3xl font-bold text-slate-900">LLM extractions & evidence</h1>
      </header>
      <section className="rounded-2xl bg-white p-6 shadow">
        <div className="flex flex-col gap-4 md:flex-row md:items-end md:justify-between">
          <div className="flex flex-wrap gap-2">
            {eligibilityFilterOptions.map((opt) => (
              <label key={opt} className="flex items-center gap-2 rounded-full border border-slate-200 px-3 py-1 text-sm">
                <input
                  type="checkbox"
                  checked={eligibilityFilter.includes(opt)}
                  onChange={(e) => {
                    setEligibilityFilter((prev) =>
                      e.target.checked ? [...prev, opt] : prev.filter((v) => v !== opt)
                    );
                  }}
                />
                {opt}
              </label>
            ))}
          </div>
          <input
            className="w-full max-w-sm"
            placeholder="Search URL, fund name, notes..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
        </div>
        {loading && <p className="mt-6 text-sm text-slate-500">Loading results...</p>}
        {error && <p className="mt-6 text-sm text-red-600">{error}</p>}
        {!loading && !error && (
          <div className="mt-6 overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead>
                <tr className="text-left text-xs uppercase text-slate-500">
                  {tableColumns.map((col) => (
                    <th key={col.accessor} className="border-b px-3 py-2 font-semibold">
                      {col.label}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {filtered.map((row, idx) => (
                  <tr key={`${row.fund_url}-${idx}`} className="border-b last:border-none">
                    {tableColumns.map((col) => (
                      <td key={col.accessor} className="px-3 py-2 align-top">
                        {col.render ? col.render(row) : row[col.accessor] || "—"}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
            {filtered.length === 0 && (
              <p className="mt-4 text-sm text-slate-500">No results match your filters yet.</p>
            )}
          </div>
        )}
      </section>
    </div>
  );
}

const eligibilityFilterOptions = [
  "Highly Eligible",
  "Eligible",
  "Possibly Eligible",
  "Low Match",
  "Not Eligible",
];

const tableColumns: {
  accessor: string;
  label: string;
  render?: (row: ResultRecord) => ReactNode;
}[] = [
  {
    accessor: "fund_name",
    label: "Fund Name",
  },
  {
    accessor: "fund_url",
    label: "Fund URL",
    render: (row: ResultRecord) => (
      <a href={row.fund_url} target="_blank" rel="noreferrer" className="text-brand underline">
        {row.fund_url}
      </a>
    ),
  },
  {
    accessor: "eligibility",
    label: "Eligibility",
  },
  {
    accessor: "application_status",
    label: "Status",
  },
  {
    accessor: "deadline",
    label: "Deadline",
  },
  {
    accessor: "funding_range",
    label: "Funding Range",
  },
  {
    accessor: "notes",
    label: "Notes",
  },
];
