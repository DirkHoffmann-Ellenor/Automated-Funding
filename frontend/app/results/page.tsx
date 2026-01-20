"use client";

import type { ReactNode } from "react";
import { Fragment, useCallback, useEffect, useMemo, useRef, useState } from "react";
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
  { accessor: "eligibility_reason", label: "Eligibility reason" },
  { accessor: "evidence", label: "Evidence" },
  { accessor: "pages_scraped", label: "Pages scraped" },
  { accessor: "visited_urls_count", label: "Visited URLs count" },
  { accessor: "extraction_timestamp", label: "Extraction timestamp" },
  { accessor: "error", label: "Error" },
  { accessor: "source_folder", label: "Source folder" },
  { accessor: "Processed", label: "Processed" },
];

const eligibilityFilterOptions = ["Highly Eligible", "Eligible", "Possibly Eligible", "Low Match", "Not Eligible"];

const columnFilterConfig = [
  {
    key: "fund",
    label: "Fund name / URL",
    accessors: ["fund_name", "fund_url"],
    placeholder: "e.g. climate, foundation",
  },
  {
    key: "applicantTypes",
    label: "Applicant types",
    accessors: ["applicant_types"],
    placeholder: "e.g. nonprofit, charity",
  },
  {
    key: "geographicScope",
    label: "Geographic scope",
    accessors: ["geographic_scope"],
    placeholder: "e.g. UK, EU",
  },
  {
    key: "beneficiaryFocus",
    label: "Beneficiary focus",
    accessors: ["beneficiary_focus"],
    placeholder: "e.g. youth, health",
  },
  {
    key: "restrictions",
    label: "Restrictions",
    accessors: ["restrictions"],
    placeholder: "e.g. match funding",
  },
  {
    key: "applicationStatus",
    label: "Status",
    accessors: ["application_status"],
    placeholder: "e.g. open, rolling",
  },
  {
    key: "notes",
    label: "Notes",
    accessors: ["notes"],
    placeholder: "keywords in notes",
  },
] as const;

type ColumnFilterKey = (typeof columnFilterConfig)[number]["key"];
type ColumnFilters = Record<ColumnFilterKey, string>;

const defaultColumnFilters: ColumnFilters = {
  fund: "",
  applicantTypes: "",
  geographicScope: "",
  beneficiaryFocus: "",
  restrictions: "",
  applicationStatus: "",
  notes: "",
};

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
  columnFilters: ColumnFilters;
  onlyFutureDeadlines: boolean;
  onlyNonprofits: boolean;
  minFunding: string;
  fundingKeywords: string;
  showEvidence: boolean;
  pinnedRowKey: string | null;
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
  const [columnFilters, setColumnFilters] = useState<ColumnFilters>(defaultColumnFilters);
  const [onlyFutureDeadlines, setOnlyFutureDeadlines] = useState(false);
  const [onlyNonprofits, setOnlyNonprofits] = useState(false);
  const [minFunding, setMinFunding] = useState("");
  const [fundingKeywords, setFundingKeywords] = useState("");
  const [showEvidence, setShowEvidence] = useState(true);
  const [selectedRowKey, setSelectedRowKey] = useState<string | null>(null);
  const [expandedRows, setExpandedRows] = useState<Set<string>>(() => new Set());
  const [pinnedRowKey, setPinnedRowKey] = useState<string | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [hydratedCache, setHydratedCache] = useState(false);
  const [hasCachedData, setHasCachedData] = useState(false);
  const [shouldForceRefresh, setShouldForceRefresh] = useState(false);
  const searchRef = useRef<HTMLInputElement>(null);

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
      const legacyPinned = (cached as { pinnedResult?: ResultRecord | null }).pinnedResult;
      setData(cached.data || []);
      setEligibilityFilter(
        cached.eligibilityFilter && cached.eligibilityFilter.length > 0
          ? cached.eligibilityFilter
          : eligibilityFilterOptions,
      );
      setSortMode(cached.sortMode || "recent");
      setSearch(cached.search || "");
      setColumnFilters({ ...defaultColumnFilters, ...(cached.columnFilters || {}) });
      setOnlyFutureDeadlines(Boolean(cached.onlyFutureDeadlines));
      setOnlyNonprofits(Boolean(cached.onlyNonprofits));
      setMinFunding(cached.minFunding || "");
      setFundingKeywords(cached.fundingKeywords || "");
      setShowEvidence(cached.showEvidence ?? true);
      setPinnedRowKey(cached.pinnedRowKey || legacyPinned?.fund_url || null);
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
      columnFilters,
      onlyFutureDeadlines,
      onlyNonprofits,
      minFunding,
      fundingKeywords,
      showEvidence,
      pinnedRowKey,
    };
    writeCache(RESULTS_CACHE_KEY, payload);
  }, [
    data,
    eligibilityFilter,
    sortMode,
    search,
    columnFilters,
    onlyFutureDeadlines,
    onlyNonprofits,
    minFunding,
    fundingKeywords,
    showEvidence,
    pinnedRowKey,
    hydratedCache,
  ]);

  const visibleResults = useMemo(() => {
    const query = search.trim().toLowerCase();
    const fundingQuery = fundingKeywords.trim().toLowerCase();
    const minFundingValue = parseCurrencyInput(minFunding);
    const now = Date.now();

    const filtered = data.filter((row) => {
      const elig = row.eligibility || "";
      const inFilter = eligibilityFilter.length === 0 || eligibilityFilter.includes(elig);
      if (!inFilter) return false;

      if (query && !matchesGlobalSearch(row, query)) return false;
      if (!matchesColumnFilters(row, columnFilters)) return false;

      if (onlyFutureDeadlines && !isFutureDeadline(row.deadline, now)) return false;
      if (onlyNonprofits && !matchesNonprofit(row.applicant_types)) return false;

      if (minFundingValue !== null) {
        const maxFunding = parseFundingRangeMax(row.funding_range);
        if (maxFunding === null || maxFunding < minFundingValue) return false;
      }

      if (fundingQuery) {
        const fundingText = [row.funding_range, row.notes, row.restrictions]
          .map((val) => normalizeText(val).toLowerCase())
          .join(" ");
        if (!fundingText.includes(fundingQuery)) return false;
      }

      return true;
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
  }, [
    data,
    eligibilityFilter,
    search,
    sortMode,
    columnFilters,
    onlyFutureDeadlines,
    onlyNonprofits,
    minFunding,
    fundingKeywords,
  ]);

  const detailFieldList = useMemo(
    () => (showEvidence ? detailFields : detailFields.filter((field) => field.accessor !== "evidence")),
    [showEvidence],
  );

  const selectedIndex = useMemo(() => {
    if (visibleResults.length === 0) return -1;
    if (!selectedRowKey) return 0;
    const idx = visibleResults.findIndex((row, index) => getRowKey(row, index) === selectedRowKey);
    return idx === -1 ? 0 : idx;
  }, [visibleResults, selectedRowKey]);

  useEffect(() => {
    if (visibleResults.length === 0) {
      if (selectedRowKey !== null) setSelectedRowKey(null);
      return;
    }
    const idx = visibleResults.findIndex((row, index) => getRowKey(row, index) === selectedRowKey);
    if (idx === -1) {
      setSelectedRowKey(getRowKey(visibleResults[0], 0));
    }
  }, [visibleResults, selectedRowKey]);

  useEffect(() => {
    if (!pinnedRowKey) return;
    const exists = data.some((row, index) => getRowKey(row, index) === pinnedRowKey);
    if (!exists) setPinnedRowKey(null);
  }, [data, pinnedRowKey]);

  const toggleExpandedRow = useCallback(
    (rowKey: string) => {
      setExpandedRows((prev) => {
        const next = new Set(prev);
        if (pinnedRowKey === rowKey) {
          next.delete(rowKey);
          return next;
        }
        if (next.has(rowKey)) {
          next.delete(rowKey);
        } else {
          next.add(rowKey);
        }
        return next;
      });
      if (pinnedRowKey === rowKey) {
        setPinnedRowKey(null);
      }
    },
    [pinnedRowKey],
  );

  const togglePinnedRow = useCallback(
    (rowKey: string) => {
      setPinnedRowKey((prev) => (prev === rowKey ? null : rowKey));
      setExpandedRows((prev) => {
        const next = new Set(prev);
        if (pinnedRowKey === rowKey) {
          next.delete(rowKey);
        } else {
          next.add(rowKey);
        }
        return next;
      });
      setSelectedRowKey(rowKey);
    },
    [pinnedRowKey],
  );

  const handleDownload = useCallback(() => {
    if (visibleResults.length === 0) return;
    const csv = buildCsv(visibleResults, exportColumns);
    downloadCsv(csv, `funding-results-${new Date().toISOString().slice(0, 10)}.csv`);
  }, [visibleResults]);

  useEffect(() => {
    const handler = (event: KeyboardEvent) => {
      if (isTextInput(event.target)) return;
      if (event.metaKey || event.ctrlKey || event.altKey) return;
      if (event.key === "ArrowDown") {
        event.preventDefault();
        if (visibleResults.length === 0) return;
        const nextIndex = Math.min(selectedIndex + 1, visibleResults.length - 1);
        setSelectedRowKey(getRowKey(visibleResults[nextIndex], nextIndex));
        return;
      }
      if (event.key === "ArrowUp") {
        event.preventDefault();
        if (visibleResults.length === 0) return;
        const nextIndex = Math.max(selectedIndex - 1, 0);
        setSelectedRowKey(getRowKey(visibleResults[nextIndex], nextIndex));
        return;
      }
      if (event.key === "Enter") {
        event.preventDefault();
        if (selectedIndex === -1) return;
        const row = visibleResults[selectedIndex];
        if (!row) return;
        togglePinnedRow(getRowKey(row, selectedIndex));
        return;
      }
      if (event.key === "f" || event.key === "F") {
        event.preventDefault();
        searchRef.current?.focus();
        return;
      }
      if (event.key === "e" || event.key === "E") {
        event.preventDefault();
        setShowEvidence((prev) => !prev);
      }
    };

    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [selectedIndex, togglePinnedRow, visibleResults]);

  return (
    <div className="space-y-6">
      <header className="space-y-1">
        <p className="text-sm uppercase tracking-wide text-neutral-500">Results</p>
        <h1 className="text-3xl font-bold text-neutral-950">LLM extractions & evidence</h1>
        <p className="text-sm text-neutral-600">
          Use column filters, expand rows for full details, and download the filtered view.
        </p>
      </header>

      <Card className="overflow-hidden">
        <div className="h-1 bg-gradient-to-r from-neutral-900 via-orange-500 to-neutral-900" />
        <CardHeader className="pb-4">
          <CardTitle className="flex flex-wrap items-center justify-between gap-3">
            <span>Funding results</span>
            <div className="flex flex-wrap items-center gap-2">
              <Button variant="outline" size="sm" onClick={() => fetchLatest({ showLoading: true })} disabled={refreshing}>
                {refreshing ? "Refreshing..." : "Refresh"}
              </Button>
              <Button
                variant="outline"
                size="sm"
                onClick={handleDownload}
                disabled={visibleResults.length === 0}
              >
                Download filtered
              </Button>
              <Button variant="outline" size="sm" onClick={() => setShowEvidence((prev) => !prev)}>
                {showEvidence ? "Hide evidence" : "Show evidence"}
              </Button>
              {!loading && <Badge variant="outline">{visibleResults.length} shown</Badge>}
            </div>
          </CardTitle>
          <CardDescription>
            Column filters, funding thresholds, and expandable rows replace the old detail panel.
          </CardDescription>
          {refreshing && <p className="text-xs text-neutral-500">Refreshing latest data...</p>}
        </CardHeader>
        <CardContent className="space-y-5">
          <div className="space-y-4">
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
                  placeholder="Search all columns..."
                  value={search}
                  onChange={(e) => setSearch(e.target.value)}
                  ref={searchRef}
                />
              </div>
            </div>

            <div className="grid gap-3 md:grid-cols-2 lg:grid-cols-4">
              <label className="flex items-center gap-2 rounded-lg border border-neutral-200 bg-white px-3 py-2 text-sm text-neutral-700">
                <Checkbox checked={onlyFutureDeadlines} onChange={(e) => setOnlyFutureDeadlines(e.target.checked)} />
                Future deadlines only
              </label>
              <label className="flex items-center gap-2 rounded-lg border border-neutral-200 bg-white px-3 py-2 text-sm text-neutral-700">
                <Checkbox checked={onlyNonprofits} onChange={(e) => setOnlyNonprofits(e.target.checked)} />
                Nonprofits OK
              </label>
              <div className="space-y-1">
                <Label htmlFor="min-funding" className="text-xs uppercase tracking-wide text-neutral-600">
                  Min funding
                </Label>
                <Input
                  id="min-funding"
                  placeholder="e.g. 50000 or 50k"
                  value={minFunding}
                  onChange={(e) => setMinFunding(e.target.value)}
                />
              </div>
              <div className="space-y-1">
                <Label htmlFor="funding-keywords" className="text-xs uppercase tracking-wide text-neutral-600">
                  Funding keywords
                </Label>
                <Input
                  id="funding-keywords"
                  placeholder="e.g. capital, equipment"
                  value={fundingKeywords}
                  onChange={(e) => setFundingKeywords(e.target.value)}
                />
              </div>
            </div>

            <div className="grid gap-3 md:grid-cols-2 lg:grid-cols-3">
              {columnFilterConfig.map((filter) => (
                <div key={filter.key} className="space-y-1">
                  <Label htmlFor={`filter-${filter.key}`} className="text-xs uppercase tracking-wide text-neutral-600">
                    {filter.label}
                  </Label>
                  <Input
                    id={`filter-${filter.key}`}
                    placeholder={filter.placeholder}
                    value={columnFilters[filter.key]}
                    onChange={(e) =>
                      setColumnFilters((prev) => ({
                        ...prev,
                        [filter.key]: e.target.value,
                      }))
                    }
                  />
                </div>
              ))}
            </div>

            <div className="flex flex-wrap items-center gap-2 text-xs text-neutral-500">
              <span className="font-semibold text-neutral-700">Shortcuts</span>
              <span>Up/Down move</span>
              <span>Enter pin</span>
              <span>F search</span>
              <span>E evidence</span>
            </div>
          </div>

          {loading && <p className="text-sm text-neutral-600">Loading results...</p>}
          {error && <p className="text-sm text-red-600">{error}</p>}

          {!loading && !error && (
            <Table>
              <TableHeader>
                <TableRow className="bg-neutral-50">
                  <TableHead className="w-12">Details</TableHead>
                  {tableColumns.map((col) => (
                    <TableHead key={col.accessor}>{col.label}</TableHead>
                  ))}
                </TableRow>
              </TableHeader>
              <TableBody>
                {visibleResults.map((row, idx) => {
                  const rowKey = getRowKey(row, idx);
                  const isSelected = selectedRowKey === rowKey;
                  const isPinned = pinnedRowKey === rowKey;
                  const isExpanded = expandedRows.has(rowKey) || isPinned;
                  return (
                    <Fragment key={`${rowKey}-${idx}`}>
                      <TableRow
                        className={`${isSelected ? "ring-1 ring-neutral-900/30 ring-inset" : ""} ${
                          isPinned ? "ring-2 ring-orange-400/70 ring-inset" : ""
                        }`}
                        onClick={() => setSelectedRowKey(rowKey)}
                      >
                        <TableCell className="w-12">
                          <Button
                            type="button"
                            variant="ghost"
                            size="icon"
                            className="h-8 w-8 border border-neutral-200 bg-white text-xs font-semibold text-neutral-700 hover:border-neutral-900"
                            aria-expanded={isExpanded}
                            title={isExpanded ? "Collapse details" : "Expand details"}
                            onClick={(event) => {
                              event.stopPropagation();
                              toggleExpandedRow(rowKey);
                            }}
                          >
                            {isExpanded ? "v" : ">"}
                          </Button>
                        </TableCell>
                        {tableColumns.map((col) => (
                          <TableCell key={col.accessor}>
                            {col.render ? col.render(row) : row[col.accessor] ?? "-"}
                          </TableCell>
                        ))}
                      </TableRow>
                      {isExpanded && (
                        <TableRow className="bg-white">
                          <TableCell colSpan={tableColumns.length + 1} className="bg-white">
                            <div className="rounded-xl border border-neutral-200 bg-white p-4 shadow-sm">
                              <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
                                <div className="space-y-1">
                                  <p className="text-xs uppercase tracking-wide text-neutral-500">Fund</p>
                                  <p className="text-base font-semibold text-neutral-900">
                                    {row.fund_name || "Unnamed fund"}
                                  </p>
                                  {row.fund_url ? (
                                    <a
                                      href={row.fund_url}
                                      target="_blank"
                                      rel="noreferrer"
                                      className="block text-xs text-neutral-600 underline decoration-neutral-300 underline-offset-4 hover:text-neutral-900"
                                    >
                                      {row.fund_url}
                                    </a>
                                  ) : (
                                    <p className="text-xs text-neutral-500">No URL provided.</p>
                                  )}
                                </div>
                                <div className="flex flex-wrap items-center gap-2">
                                  <Badge variant={eligibilityTone[row.eligibility] || "outline"}>
                                    {row.eligibility || "Unknown"}
                                  </Badge>
                                  {isPinned && <Badge variant="outline">Pinned</Badge>}
                                  {isPinned && (
                                    <button
                                      className="text-xs text-neutral-500 underline"
                                      onClick={() => togglePinnedRow(rowKey)}
                                    >
                                      Clear pin
                                    </button>
                                  )}
                                </div>
                              </div>
                              {!showEvidence && (
                                <p className="text-xs text-neutral-500">Evidence hidden. Press E to show it.</p>
                              )}
                              <div className="grid gap-3 md:grid-cols-2">
                                {detailFieldList.map((field) => (
                                  <div
                                    key={field.accessor}
                                    className="rounded-lg border border-neutral-200 bg-neutral-50 px-3 py-2"
                                  >
                                    <p className="text-[11px] uppercase tracking-wide text-neutral-500">{field.label}</p>
                                    <p className="text-sm text-neutral-900">
                                      {formatValue(row[field.accessor as keyof ResultRecord])}
                                    </p>
                                  </div>
                                ))}
                              </div>
                            </div>
                          </TableCell>
                        </TableRow>
                      )}
                    </Fragment>
                  );
                })}
              </TableBody>
              {visibleResults.length === 0 && <TableCaption>No results match your filters yet.</TableCaption>}
            </Table>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function getEligibilityRank(value: string) {
  const idx = eligibilityFilterOptions.indexOf(value);
  return idx === -1 ? eligibilityFilterOptions.length : idx;
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

function normalizeText(val: any) {
  if (val === null || val === undefined) return "";
  if (Array.isArray(val)) return val.join(" ");
  return String(val);
}

function matchesGlobalSearch(row: ResultRecord, query: string) {
  if (!query) return true;
  return Object.values(row).some((val) => normalizeText(val).toLowerCase().includes(query));
}

function matchesColumnFilters(row: ResultRecord, filters: ColumnFilters) {
  return columnFilterConfig.every((filter) => {
    const query = filters[filter.key]?.trim().toLowerCase();
    if (!query) return true;
    return filter.accessors.some((accessor) => normalizeText(row[accessor]).toLowerCase().includes(query));
  });
}

const nonprofitKeywords = [
  "nonprofit",
  "non-profit",
  "non profit",
  "charity",
  "charitable",
  "not-for-profit",
  "not for profit",
  "ngo",
];

function matchesNonprofit(value: any) {
  const text = normalizeText(value).toLowerCase();
  if (!text) return false;
  return nonprofitKeywords.some((keyword) => text.includes(keyword));
}

function parseCurrencyInput(value: string) {
  if (!value) return null;
  const cleaned = value.replace(/,/g, "").trim().toLowerCase();
  const match = cleaned.match(/(\d+(?:\.\d+)?)(\s*[km])?/);
  if (!match) return null;
  let amount = Number.parseFloat(match[1]);
  const suffix = match[2]?.trim();
  if (suffix === "k") amount *= 1000;
  if (suffix === "m") amount *= 1000000;
  return Number.isNaN(amount) ? null : amount;
}

function parseFundingRangeMax(value: any) {
  const text = normalizeText(value).toLowerCase();
  if (!text) return null;
  const regex = /(\d+(?:\.\d+)?)(\s*[km])?/g;
  const amounts: number[] = [];
  let match: RegExpExecArray | null;
  while ((match = regex.exec(text)) !== null) {
    let amount = Number.parseFloat(match[1]);
    const suffix = match[2]?.trim();
    if (suffix === "k") amount *= 1000;
    if (suffix === "m") amount *= 1000000;
    if (Number.isFinite(amount)) amounts.push(amount);
  }
  if (amounts.length === 0) return null;
  const maxAmount = Math.max(...amounts);
  return Number.isFinite(maxAmount) ? maxAmount : null;
}

function isFutureDeadline(value: any, now: number) {
  if (!value) return false;
  const text = normalizeText(value).toLowerCase();
  if (text.includes("rolling") || text.includes("ongoing") || text.includes("open")) {
    return true;
  }
  const timestamp = parseDateValue(value);
  return timestamp !== null && timestamp >= now;
}

function getRowKey(row: ResultRecord, idx: number) {
  return row.fund_url || row.fund_name || row.source_folder || row.extraction_timestamp || `row-${idx}`;
}

function isTextInput(target: EventTarget | null) {
  if (!target) return false;
  const element = target as HTMLElement;
  if (element.isContentEditable) return true;
  return Boolean(element.closest("input, textarea, select, button, a"));
}

const exportColumns = [
  { accessor: "fund_url", label: "fund_url" },
  { accessor: "fund_name", label: "fund_name" },
  { accessor: "applicant_types", label: "applicant_types" },
  { accessor: "geographic_scope", label: "geographic_scope" },
  { accessor: "beneficiary_focus", label: "beneficiary_focus" },
  { accessor: "funding_range", label: "funding_range" },
  { accessor: "restrictions", label: "restrictions" },
  { accessor: "application_status", label: "application_status" },
  { accessor: "deadline", label: "deadline" },
  { accessor: "notes", label: "notes" },
  { accessor: "eligibility", label: "eligibility" },
  { accessor: "evidence", label: "evidence" },
  { accessor: "pages_scraped", label: "pages_scraped" },
  { accessor: "visited_urls_count", label: "visited_urls_count" },
  { accessor: "extraction_timestamp", label: "extraction_timestamp" },
  { accessor: "error", label: "error" },
  { accessor: "source_folder", label: "source_folder" },
  { accessor: "Processed", label: "Processed" },
];

function formatCsvValue(value: any) {
  if (value === null || value === undefined) return "";
  return Array.isArray(value) ? value.join(", ") : String(value);
}

function escapeCsvValue(value: string) {
  const escaped = value.replace(/"/g, '""');
  return /[",\n]/.test(escaped) ? `"${escaped}"` : escaped;
}

function buildCsv(rows: ResultRecord[], columns: { accessor: string; label: string }[]) {
  const header = columns.map((col) => escapeCsvValue(col.label)).join(",");
  const lines = rows.map((row) =>
    columns.map((col) => escapeCsvValue(formatCsvValue(row[col.accessor]))).join(","),
  );
  return [header, ...lines].join("\r\n");
}

function downloadCsv(csv: string, filename: string) {
  const blob = new Blob([csv], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const link = document.createElement("a");
  link.href = url;
  link.download = filename;
  document.body.appendChild(link);
  link.click();
  document.body.removeChild(link);
  URL.revokeObjectURL(url);
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
      <div className="space-y-1 min-w-0 min-h-[4.5rem]">
        <div className="flex items-center gap-2 min-w-0">
          <p className="min-w-0 text-sm font-semibold text-neutral-900 truncate">
            {row.fund_name || "Unnamed fund"}
          </p>
          <Badge
            variant={eligibilityTone[row.eligibility] || "outline"}
            className="shrink-0 whitespace-nowrap"
          >
            {row.eligibility || "Unknown"}
          </Badge>
        </div>
        {row.fund_url ? (
          <a
            href={row.fund_url}
            target="_blank"
            rel="noreferrer"
            className="block min-w-0 truncate text-xs text-neutral-600 underline decoration-neutral-300 underline-offset-4 hover:text-neutral-900"
          >
            {row.fund_url}
          </a>
        ) : (
          <p className="text-xs text-neutral-500">No URL provided.</p>
        )}
        {row.notes && <p className="text-xs text-neutral-600 truncate">{row.notes}</p>}
      </div>
    ),
  },
  {
    accessor: "applicant_types",
    label: "Audience & scope",
    render: (row: ResultRecord) => (
      <div className="space-y-1 text-xs text-neutral-600 min-h-[4.5rem]">
        <p className="truncate">
          <span className="font-semibold text-neutral-800">Applicants:</span> {formatValue(row.applicant_types)}
        </p>
        <p className="truncate">
          <span className="font-semibold text-neutral-800">Focus:</span> {formatValue(row.beneficiary_focus)}
        </p>
        <p className="truncate">
          <span className="font-semibold text-neutral-800">Scope:</span> {formatValue(row.geographic_scope)}
        </p>
      </div>
    ),
  },
  {
    accessor: "funding_range",
    label: "Funding",
    render: (row: ResultRecord) => (
      <div className="space-y-1 min-h-[4.5rem]">
        <p className="text-sm font-semibold text-neutral-900 truncate">
          {row.funding_range || "Range not provided"}
        </p>
        {row.restrictions && (
          <p className="text-xs text-neutral-600 truncate">Restrictions: {row.restrictions}</p>
        )}
      </div>
    ),
  },
  {
    accessor: "application_status",
    label: "Status & deadline",
    render: (row: ResultRecord) => (
      <div className="space-y-1 min-h-[4.5rem]">
        <Badge variant="muted" className="w-fit">
          {row.application_status || "Not stated"}
        </Badge>
        <p className="text-xs text-neutral-600 truncate">Deadline: {row.deadline || "Not listed"}</p>
      </div>
    ),
  },
  {
    accessor: "source_folder",
    label: "Source",
    render: (row: ResultRecord) => (
      <div className="space-y-1 min-h-[4.5rem]">
        <p className="text-sm font-semibold text-neutral-900 truncate">{row.source_folder || "Not listed"}</p>
        <p className="text-xs text-neutral-600 truncate">Pages: {row.pages_scraped ?? "-"}</p>
        <p className="text-xs text-neutral-600 truncate">Visited: {row.visited_urls_count ?? "-"}</p>
      </div>
    ),
  },
];
