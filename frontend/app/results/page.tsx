"use client";

import type { ReactNode } from "react";
import { useCallback, useEffect, useMemo, useState } from "react";
import { Badge } from "../../components/ui/badge";
import { Button } from "../../components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "../../components/ui/card";
import { Checkbox } from "../../components/ui/checkbox";
import { Input } from "../../components/ui/input";
import { Label } from "../../components/ui/label";
import {
  Table,
  TableBody,
  TableCaption,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "../../components/ui/table";
import { api } from "../../lib/api";
import { clearCache, readCache, writeCache } from "../../lib/storage";

type ResultRecord = Record<string, any>;
type SortMode = "recent" | "alphabetical" | "eligibility";

const eligibilityTone: Record<string, "accent" | "muted" | "outline"> = {
  "Highly Eligible": "accent",
  Eligible: "accent",
  "Possibly Eligible": "muted",
  "Low Match": "outline",
  "Not Eligible": "outline",
};

const eligibilityRowTone: Record<string, { base: string; hover: string }> = {
  "Highly Eligible": { base: "bg-emerald-200", hover: "hover:bg-emerald-300" },
  Eligible: { base: "bg-green-200", hover: "hover:bg-green-300" },
  "Possibly Eligible": { base: "bg-amber-200", hover: "hover:bg-amber-300" },
  "Low Match": { base: "bg-orange-200", hover: "hover:bg-orange-300" },
  "Not Eligible": { base: "bg-rose-200", hover: "hover:bg-rose-300" },
};

const detailFields = [
  { accessor: "applicant_types", label: "Applicant types" },
  { accessor: "geographic_scope", label: "Geographic scope" },
  { accessor: "beneficiary_focus", label: "Beneficiary focus" },
  { accessor: "funding_range", label: "Funding range" },
  { accessor: "restrictions", label: "Restrictions" },
  { accessor: "application_status", label: "Application status" },
  { accessor: "deadline", label: "Deadline" },
  { accessor: "notes", label: "Notes" },
  { accessor: "eligibility", label: "Eligibility" },
  { accessor: "evidence", label: "Evidence" },
  { accessor: "pages_scraped", label: "Pages scraped" },
  { accessor: "visited_urls_count", label: "Visited URLs" },
  { accessor: "extraction_timestamp", label: "Extraction timestamp" },
  { accessor: "error", label: "Error" },
];

const eligibilityFilterOptions = ["Highly Eligible", "Eligible", "Possibly Eligible", "Low Match", "Not Eligible"];

const sortOptions: { value: SortMode; label: string }[] = [
  { value: "recent", label: "Most recent" },
  { value: "alphabetical", label: "Alphabetical (A-Z)" },
  { value: "eligibility", label: "Eligibility (best first)" },
];

type ResultsCache = {
  data: ResultRecord[];
  eligibilityFilter: string[];
  sortMode: SortMode;
  search: string;
  pinnedResult: ResultRecord | null;
};

const RESULTS_CACHE_KEY = "results_cache_v1";
const RESULTS_FORCE_REFRESH_KEY = "results_force_refresh_v1";

export default function ResultsPage() {
  const [data, setData] = useState<ResultRecord[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [eligibilityFilter, setEligibilityFilter] = useState<string[]>([]);
  const [sortMode, setSortMode] = useState<SortMode>("recent");
  const [search, setSearch] = useState("");
  const [hoveredResult, setHoveredResult] = useState<ResultRecord | null>(null);
  const [pinnedResult, setPinnedResult] = useState<ResultRecord | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [hydratedCache, setHydratedCache] = useState(false);
  const [hasCachedData, setHasCachedData] = useState(false);
  const [shouldForceRefresh, setShouldForceRefresh] = useState(false);

  const fetchLatest = useCallback(
    async (opts?: { showLoading?: boolean }) => {
      const showLoading = opts?.showLoading ?? false;
      setRefreshing(true);
      if (showLoading) setLoading(true);
      try {
        const res = await api.results();
        setData(res.results || []);
        setHasCachedData(Boolean(res.results && res.results.length));
        setEligibilityFilter((prev) => (prev.length === 0 ? eligibilityFilterOptions : prev));
        setError(null);
      } catch (err: any) {
        setError(err.message);
      } finally {
        setLoading(false);
        setRefreshing(false);
        clearCache(RESULTS_FORCE_REFRESH_KEY);
        setShouldForceRefresh(false);
      }
    },
    [],
  );

  useEffect(() => {
    const cached = readCache<ResultsCache>(RESULTS_CACHE_KEY)?.value;
    if (cached) {
      setData(cached.data || []);
      setEligibilityFilter(
        cached.eligibilityFilter && cached.eligibilityFilter.length > 0
          ? cached.eligibilityFilter
          : eligibilityFilterOptions,
      );
      setSortMode(cached.sortMode || "recent");
      setSearch(cached.search || "");
      setPinnedResult(cached.pinnedResult || null);
      setHasCachedData(Boolean(cached.data && cached.data.length));
      setLoading(false);
    } else {
      setEligibilityFilter(eligibilityFilterOptions);
    }
    const refreshFlag = readCache<{ jobId?: string; completedAt?: number }>(RESULTS_FORCE_REFRESH_KEY)?.value;
    if (refreshFlag) setShouldForceRefresh(true);
    setHydratedCache(true);
  }, []);

  useEffect(() => {
    if (!hydratedCache || shouldForceRefresh) return;
    fetchLatest({ showLoading: !hasCachedData });
  }, [hydratedCache, hasCachedData, shouldForceRefresh, fetchLatest]);

  useEffect(() => {
    if (!hydratedCache || !shouldForceRefresh) return;
    fetchLatest({ showLoading: true });
  }, [hydratedCache, shouldForceRefresh, fetchLatest]);

  useEffect(() => {
    if (!hydratedCache) return;
    const payload: ResultsCache = {
      data,
      eligibilityFilter,
      sortMode,
      search,
      pinnedResult,
    };
    writeCache(RESULTS_CACHE_KEY, payload);
  }, [data, eligibilityFilter, sortMode, search, pinnedResult, hydratedCache]);

  const visibleResults = useMemo(() => {
    const filtered = data.filter((row) => {
      const elig = row.eligibility || "";
      const inFilter = eligibilityFilter.length === 0 || eligibilityFilter.includes(elig);
      const query = search.toLowerCase();
      const matchesQuery =
        !query || Object.values(row).some((val) => (val || "").toString().toLowerCase().includes(query));
      return inFilter && matchesQuery;
    });

    const sorted = [...filtered].sort((a, b) => {
      if (sortMode === "alphabetical") {
        const aVal = (a.fund_name || a.fund_url || "").toString();
        const bVal = (b.fund_name || b.fund_url || "").toString();
        return aVal.localeCompare(bVal, undefined, { sensitivity: "base" });
      }

      if (sortMode === "eligibility") {
        return getEligibilityRank(a.eligibility) - getEligibilityRank(b.eligibility);
      }

      return getSortTimestamp(b) - getSortTimestamp(a);
    });

    return sorted;
  }, [data, eligibilityFilter, search, sortMode]);

  const activeResult = pinnedResult || hoveredResult || (visibleResults.length > 0 ? visibleResults[0] : null);

  const clearPin = () => setPinnedResult(null);

  return (
    <div className="space-y-6">
      <header className="space-y-1">
        <p className="text-sm uppercase tracking-wide text-neutral-500">Results</p>
        <h1 className="text-3xl font-bold text-neutral-950">LLM extractions & evidence</h1>
        <p className="text-sm text-neutral-600">Hover or click a row to inspect the full scraped details.</p>
      </header>

      <div className="grid gap-4 lg:grid-cols-[2fr,1fr] items-start">
        <Card className="overflow-hidden">
          <div className="h-1 bg-gradient-to-r from-neutral-900 via-orange-500 to-neutral-900" />
          <CardHeader className="pb-4">
            <CardTitle className="flex items-center justify-between gap-3">
              <span>Funding results</span>
              <div className="flex items-center gap-2">
                <Button variant="outline" size="sm" onClick={() => fetchLatest({ showLoading: true })} disabled={refreshing}>
                  {refreshing ? "Refreshing..." : "Refresh"}
                </Button>
                {!loading && <Badge variant="outline">{visibleResults.length} shown</Badge>}
              </div>
            </CardTitle>
            <CardDescription>Filter by eligibility, search text, and sort alphabetically or by recency before diving in.</CardDescription>
            {refreshing && <p className="text-xs text-neutral-500">Refreshing latest data...</p>}
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="flex flex-col gap-3">
              <div className="space-y-2">
                <p className="text-xs uppercase tracking-wide text-neutral-600">Eligibility</p>
                <div className="flex flex-wrap gap-2">
                  {eligibilityFilterOptions.map((opt) => {
                    const active = eligibilityFilter.includes(opt);
                    return (
                      <label
                        key={opt}
                        className={`flex items-center gap-2 rounded-full border px-3 py-1 text-xs font-semibold uppercase tracking-wide transition ${
                          active
                            ? "border-neutral-900 bg-neutral-900 text-white"
                            : "border-neutral-200 bg-white text-neutral-700 hover:border-neutral-900"
                        }`}
                      >
                        <Checkbox
                          checked={active}
                          onChange={(e) =>
                            setEligibilityFilter((prev) =>
                              e.target.checked ? [...prev, opt] : prev.filter((v) => v !== opt),
                            )
                          }
                        />
                        {opt}
                      </label>
                    );
                  })}
                </div>
              </div>

              <div className="flex flex-col gap-3 md:flex-row md:items-end md:justify-between">
                <div className="w-full max-w-xs space-y-1">
                  <Label htmlFor="result-sort" className="text-xs uppercase tracking-wide text-neutral-600">
                    Sort by
                  </Label>
                  <select
                    id="result-sort"
                    value={sortMode}
                    onChange={(e) => setSortMode(e.target.value as SortMode)}
                    className="h-10 w-full rounded-md border border-neutral-300 bg-white px-3 text-sm shadow-sm outline-none transition focus:border-neutral-900 focus:ring-2 focus:ring-neutral-900/10"
                  >
                    {sortOptions.map((opt) => (
                      <option key={opt.value} value={opt.value}>
                        {opt.label}
                      </option>
                    ))}
                  </select>
                </div>
                <div className="w-full max-w-sm space-y-1">
                  <Label htmlFor="result-search" className="text-xs uppercase tracking-wide text-neutral-600">
                    Search
                  </Label>
                  <Input
                    id="result-search"
                    placeholder="Search URL, fund name, notes..."
                    value={search}
                    onChange={(e) => setSearch(e.target.value)}
                  />
                </div>
              </div>
            </div>

            {loading && <p className="text-sm text-neutral-600">Loading results...</p>}
            {error && <p className="text-sm text-red-600">{error}</p>}

            {!loading && !error && (
              <Table>
                <TableHeader>
                  <TableRow className="bg-neutral-50">
                    {tableColumns.map((col) => (
                      <TableHead key={col.accessor}>{col.label}</TableHead>
                    ))}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {visibleResults.map((row, idx) => {
                    const isActive = activeResult?.fund_url === row.fund_url;
                    const isPinned = pinnedResult?.fund_url === row.fund_url;
                    const tone = getEligibilityRowTone(row.eligibility);
                    return (
                      <TableRow
                        key={`${row.fund_url || "row"}-${idx}`}
                        className={`${tone.base} ${tone.hover} ${isActive ? "ring-1 ring-neutral-900/30" : ""} ${
                          isPinned ? "ring-2 ring-orange-400/70" : ""
                        }`}
                        onMouseEnter={() => setHoveredResult(row)}
                        onMouseLeave={() => setHoveredResult(null)}
                        onClick={() => setPinnedResult(isPinned ? null : row)}
                        title="Hover to preview; click to pin details"
                      >
                        {tableColumns.map((col) => (
                          <TableCell key={col.accessor}>
                            {col.render ? col.render(row) : row[col.accessor] ?? "-"}
                          </TableCell>
                        ))}
                      </TableRow>
                    );
                  })}
                </TableBody>
                {visibleResults.length === 0 && <TableCaption>No results match your filters yet.</TableCaption>}
              </Table>
            )}
          </CardContent>
        </Card>

        <Card className="self-start sticky top-4 lg:max-h-[calc(100vh-2rem)]">
          <CardHeader className="pb-3">
            <CardTitle>Details</CardTitle>
            <CardDescription>
              Hover a row for a quick preview or click to pin. All fields below come directly from the results sheet.
            </CardDescription>
          </CardHeader>
          <CardContent className="space-y-3 max-h-[calc(100vh-7rem)] overflow-y-auto pr-1 lg:pr-2">
            {!activeResult && <p className="text-sm text-neutral-600">No row selected yet.</p>}
            {activeResult && (
              <>
                <div className="flex items-start justify-between gap-3">
                  <div className="space-y-1">
                    <p className="text-xs uppercase tracking-wide text-neutral-500">Fund</p>
                    <p className="text-base font-semibold text-neutral-900">{activeResult.fund_name || "Unnamed fund"}</p>
                    <a
                      href={activeResult.fund_url}
                      target="_blank"
                      rel="noreferrer"
                      className="block text-xs text-neutral-600 underline decoration-neutral-300 underline-offset-4 hover:text-neutral-900"
                    >
                      {activeResult.fund_url}
                    </a>
                  </div>
                  <div className="flex flex-col items-end gap-2">
                    <Badge variant={eligibilityTone[activeResult.eligibility] || "outline"}>
                      {activeResult.eligibility || "Unknown"}
                    </Badge>
                    {pinnedResult && (
                      <button className="text-xs text-neutral-500 underline" onClick={clearPin}>
                        Clear pin
                      </button>
                    )}
                  </div>
                </div>
                <div className="grid gap-3">
                  {detailFields.map((field) => (
                    <div key={field.accessor} className="rounded-lg border border-neutral-200 bg-neutral-50 px-3 py-2">
                      <p className="text-[11px] uppercase tracking-wide text-neutral-500">{field.label}</p>
                      <p className="text-sm text-neutral-900">
                        {formatValue(activeResult[field.accessor as keyof ResultRecord])}
                      </p>
                    </div>
                  ))}
                </div>
              </>
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

function getEligibilityRank(value: string) {
  const idx = eligibilityFilterOptions.indexOf(value);
  return idx === -1 ? eligibilityFilterOptions.length : idx;
}

function getEligibilityRowTone(value: string | null | undefined) {
  if (!value) return { base: "bg-white", hover: "hover:bg-white" };
  return eligibilityRowTone[value] ?? { base: "bg-white", hover: "hover:bg-white" };
}

function parseDateValue(val: any) {
  if (!val) return null;
  const date = new Date(val);
  const time = date.getTime();
  return Number.isNaN(time) ? null : time;
}

function getSortTimestamp(row: ResultRecord) {
  const extractionTime = parseDateValue(row.extraction_timestamp);
  if (extractionTime !== null) return extractionTime;

  const deadlineTime = parseDateValue(row.deadline);
  return deadlineTime !== null ? deadlineTime : 0;
}

function formatValue(val: any) {
  if (val === null || val === undefined || val === "") return "-";
  return Array.isArray(val) ? val.join(", ") : String(val);
}

const tableColumns: {
  accessor: string;
  label: string;
  render?: (row: ResultRecord) => ReactNode;
}[] = [
  {
    accessor: "fund_name",
    label: "Fund",
    render: (row: ResultRecord) => (
      <div className="space-y-1">
        <p className="text-sm font-semibold text-neutral-900">{row.fund_name || "Unnamed fund"}</p>
        <a
          href={row.fund_url}
          target="_blank"
          rel="noreferrer"
          className="text-xs text-neutral-600 underline decoration-neutral-300 underline-offset-4 hover:text-neutral-900"
        >
          {row.fund_url}
        </a>
        {row.notes && <p className="text-xs text-neutral-600">{row.notes}</p>}
      </div>
    ),
  },
  {
    accessor: "eligibility",
    label: "Eligibility",
    render: (row: ResultRecord) => (
      <Badge variant={eligibilityTone[row.eligibility] || "outline"} className="whitespace-nowrap">
        {row.eligibility || "Unknown"}
      </Badge>
    ),
  },
  {
    accessor: "application_status",
    label: "Status",
    render: (row: ResultRecord) => (
      <div className="space-y-1">
        <Badge variant="muted" className="w-fit">
          {row.application_status || "Not stated"}
        </Badge>
        <p className="text-xs text-neutral-600">Deadline: {row.deadline || "Not listed"}</p>
      </div>
    ),
  },
  {
    accessor: "funding_range",
    label: "Funding & notes",
    render: (row: ResultRecord) => (
      <div className="space-y-1">
        <p className="text-sm font-semibold text-neutral-900">{row.funding_range || "Range not provided"}</p>
        {row.eligibility_reason && <p className="text-xs text-neutral-600">Reason: {row.eligibility_reason}</p>}
      </div>
    ),
  },
  {
    accessor: "pages_scraped",
    label: "Pages",
    render: (row: ResultRecord) => (
      <div className="space-y-1">
        <p className="text-sm font-semibold text-neutral-900">{row.pages_scraped ?? "-"}</p>
        <p className="text-xs text-neutral-600">Visited: {row.visited_urls_count ?? "-"}</p>
      </div>
    ),
  },
];
