"use client";

import type { ChangeEvent } from "react";
import { useEffect, useMemo, useRef, useState } from "react";
import { api } from "../../../lib/api";
import { writeCache } from "../../../lib/storage";
import { Badge } from "../../../components/ui/badge";
import { Button } from "../../../components/ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "../../../components/ui/card";
import { Label } from "../../../components/ui/label";
import { Progress } from "../../../components/ui/progress";

const RESULTS_FORCE_REFRESH_KEY = "results_force_refresh_v1";

type ResultRecord = Record<string, any>;

type JobStatus = {
  job_id: string;
  done: boolean;
  progress_percent: number;
  results: any[];
  errors: { url: string; message: string }[];
  current_url?: string | null;
  current_elapsed_seconds?: number;
  total_elapsed_seconds?: number;
  started_at?: number | null;
  finished_at?: number | null;
  url_timings?: {
    url: string;
    duration_seconds: number;
    started_at?: number | null;
    finished_at?: number | null;
    error?: string | null;
  }[];
  total_urls?: number;
  completed_urls?: number;
};

export default function ExpiredScrapedPage() {
  const [staleFunds, setStaleFunds] = useState<ResultRecord[]>([]);
  const [charityPdfFunds, setCharityPdfFunds] = useState<ResultRecord[]>([]);
  const [staleSelection, setStaleSelection] = useState<string[]>([]);
  const [charityPdfSelection, setCharityPdfSelection] = useState<string[]>([]);
  const [queuedUrls, setQueuedUrls] = useState<string[]>([]);
  const [staleLoading, setStaleLoading] = useState(false);
  const [charityPdfLoading, setCharityPdfLoading] = useState(false);
  const [staleError, setStaleError] = useState<string | null>(null);
  const [charityPdfError, setCharityPdfError] = useState<string | null>(null);
  const [cutoffTimestamp, setCutoffTimestamp] = useState<string | null>(null);
  const [job, setJob] = useState<JobStatus | null>(null);
  const [jobError, setJobError] = useState<string | null>(null);
  const [isScraping, setIsScraping] = useState(false);
  const lastCompletedJobId = useRef<string | null>(null);

  const latestStaleFunds = useMemo(() => {
    const latestByKey = new Map<string, ResultRecord>();
    staleFunds.forEach((row) => {
      const key = normalizeUrlClient(row?.fund_url) || row?.fund_url || row?.fund_name || "";
      if (!key) return;
      const current = latestByKey.get(key);
      if (!current || compareTimestampValues(row?.extraction_timestamp, current?.extraction_timestamp) > 0) {
        latestByKey.set(key, row);
      }
    });
    return Array.from(latestByKey.values());
  }, [staleFunds]);

  const latestCharityPdfFunds = useMemo(() => {
    const latestByKey = new Map<string, ResultRecord>();
    charityPdfFunds.forEach((row) => {
      const key = normalizeUrlClient(row?.fund_url) || row?.fund_url || row?.fund_name || "";
      if (!key) return;
      const current = latestByKey.get(key);
      if (!current || compareTimestampValues(row?.extraction_timestamp, current?.extraction_timestamp) > 0) {
        latestByKey.set(key, row);
      }
    });
    return Array.from(latestByKey.values());
  }, [charityPdfFunds]);

  const staleOptions = useMemo(() => {
    const seen = new Set<string>();
    const queuedSet = new Set(queuedUrls);
    const options: { value: string; label: string; disabled: boolean }[] = [];

    latestStaleFunds.forEach((row) => {
      const normalized = normalizeUrlClient(row?.fund_url);
      const value = normalized || row?.fund_url || row?.fund_name || "";
      if (!value || seen.has(value)) return;
      seen.add(value);
      const labelBase = row?.fund_name
        ? row?.fund_url
          ? `${row.fund_name} - ${row.fund_url}`
          : row.fund_name
        : row?.fund_url || row?.fund_name || "Unknown fund";
      const dateLabel = formatTimestamp(row?.extraction_timestamp);
      const ageLabel = formatAgeLabel(row?.extraction_timestamp);
      const suffixParts = [] as string[];
      if (dateLabel && dateLabel !== "-") suffixParts.push(`last scraped ${dateLabel}`);
      if (ageLabel) suffixParts.push(`(${ageLabel})`);
      const suffix = suffixParts.length > 0 ? ` - ${suffixParts.join(" ")}` : "";
      options.push({ value, label: `${labelBase}${suffix}`, disabled: queuedSet.has(value) });
    });

    return options;
  }, [latestStaleFunds, queuedUrls]);

  const charityPdfOptions = useMemo(() => {
    const seen = new Set<string>();
    const queuedSet = new Set(queuedUrls);
    const options: { value: string; label: string; disabled: boolean }[] = [];

    latestCharityPdfFunds.forEach((row) => {
      const normalized = normalizeUrlClient(row?.fund_url);
      const value = normalized || row?.fund_url || row?.fund_name || "";
      if (!value || seen.has(value)) return;
      seen.add(value);
      const labelBase = row?.fund_name
        ? row?.fund_url
          ? `${row.fund_name} - ${row.fund_url}`
          : row.fund_name
        : row?.fund_url || row?.fund_name || "Unknown fund";
      const dateLabel = formatTimestamp(row?.extraction_timestamp);
      const issueLabel = getCharityPdfIssueLabel(row);
      const suffixParts = [issueLabel] as string[];
      if (dateLabel && dateLabel !== "-") suffixParts.push(`last scraped ${dateLabel}`);
      options.push({ value, label: `${labelBase} - ${suffixParts.join(" - ")}`, disabled: queuedSet.has(value) });
    });

    return options;
  }, [latestCharityPdfFunds, queuedUrls]);

  const queueLookup = useMemo(() => {
    const map = new Map<string, ResultRecord>();
    latestStaleFunds.forEach((row) => {
      const key = normalizeUrlClient(row?.fund_url) || row?.fund_url || row?.fund_name;
      if (key && !map.has(key)) map.set(key, row);
    });
    latestCharityPdfFunds.forEach((row) => {
      const key = normalizeUrlClient(row?.fund_url) || row?.fund_url || row?.fund_name;
      if (key && !map.has(key)) map.set(key, row);
    });
    return map;
  }, [latestStaleFunds, latestCharityPdfFunds]);

  const loadStaleFunds = async (forceRefresh = false) => {
    setStaleLoading(true);
    setStaleError(null);
    try {
      const res = await api.staleResults(3, { forceRefresh });
      setStaleFunds(res.results || []);
      setCutoffTimestamp(res.cutoff_timestamp || null);
    } catch (err: any) {
      setStaleError(err.message || "Failed to load expired scrapes.");
    } finally {
      setStaleLoading(false);
    }
  };

  const loadCharityPdfFunds = async (forceRefresh = false) => {
    setCharityPdfLoading(true);
    setCharityPdfError(null);
    try {
      const res = await api.results({ forceRefresh });
      const filtered = (res.results || []).filter((row) => isCharityCommissionPdfCandidate(row));
      setCharityPdfFunds(filtered);
    } catch (err: any) {
      setCharityPdfError(err.message || "Failed to load Charity Commission PDF retries.");
    } finally {
      setCharityPdfLoading(false);
    }
  };

  useEffect(() => {
    loadStaleFunds();
    loadCharityPdfFunds();
  }, []);

  useEffect(() => {
    if (!job || !job.done) return;
    if (lastCompletedJobId.current === job.job_id) return;
    lastCompletedJobId.current = job.job_id;
    writeCache(RESULTS_FORCE_REFRESH_KEY, { jobId: job.job_id, completedAt: Date.now() });
  }, [job]);

  useEffect(() => {
    if (!job || job.done) return;
    const interval = setInterval(async () => {
      try {
        const status = await api.jobStatus(job.job_id);
        setJob(status);
      } catch (err) {
        console.error(err);
      }
    }, 4000);
    return () => clearInterval(interval);
  }, [job]);

  const handleSelectionChange = (event: ChangeEvent<HTMLSelectElement>) => {
    const selected = Array.from(event.target.selectedOptions).map((opt) => opt.value);
    setStaleSelection(selected);
  };

  const handleCharityPdfSelectionChange = (event: ChangeEvent<HTMLSelectElement>) => {
    const selected = Array.from(event.target.selectedOptions).map((opt) => opt.value);
    setCharityPdfSelection(selected);
  };

  const queueSelection = () => {
    if (staleSelection.length === 0) return;
    setQueuedUrls((prev) => {
      const next = [...prev];
      staleSelection.forEach((url) => {
        if (!next.includes(url)) next.push(url);
      });
      return next;
    });
    setStaleSelection([]);
  };

  const queueCharityPdfSelection = () => {
    if (charityPdfSelection.length === 0) return;
    setQueuedUrls((prev) => {
      const next = [...prev];
      charityPdfSelection.forEach((url) => {
        if (!next.includes(url)) next.push(url);
      });
      return next;
    });
    setCharityPdfSelection([]);
  };

  const removeFromQueue = (url: string) => {
    setQueuedUrls((prev) => prev.filter((u) => u !== url));
  };

  const clearQueue = () => {
    setQueuedUrls([]);
  };

  const startRescrape = async () => {
    if (queuedUrls.length === 0) return;
    setIsScraping(true);
    setJobError(null);
    try {
      const payload = await api.scrapeBatch([], queuedUrls, { rescrapeScope: "any" });
      const status = await api.jobStatus(payload.job_id);
      setJob(status);
      setQueuedUrls([]);
      setStaleSelection([]);
      setCharityPdfSelection([]);
    } catch (err: any) {
      setJobError(err.message || "Failed to start rescrape job.");
    } finally {
      setIsScraping(false);
    }
  };

  const clearCompletedJob = () => {
    setJob(null);
    setJobError(null);
    lastCompletedJobId.current = null;
  };

  return (
    <div className="space-y-8">
      <header>
        <p className="text-sm uppercase tracking-wide text-neutral-500">Results</p>
        <h1 className="text-3xl font-bold text-neutral-950">Expired Funds</h1>
        <p className="mt-2 text-sm text-neutral-600">
          Select stale funds, queue them up, and monitor the rescrape progress in real time.
        </p>
      </header>

      <Card className="relative overflow-hidden">
        <div className="absolute inset-x-0 top-0 h-1 bg-gradient-to-r from-neutral-900 via-orange-500 to-neutral-900" />
        <CardHeader className="pb-2">
          <CardTitle className="flex flex-wrap items-center justify-between gap-3">
            <span>Expired scraped funds</span>
            <div className="flex items-center gap-2">
              <Button variant="outline" size="sm" onClick={() => loadStaleFunds(true)} disabled={staleLoading}>
                {staleLoading ? "Refreshing..." : "Refresh list"}
              </Button>
              {latestStaleFunds.length > 0 && <Badge variant="outline">{latestStaleFunds.length} expired</Badge>}
            </div>
          </CardTitle>
          <CardDescription>
            Older than 3 months based on extraction timestamp.
            {cutoffTimestamp ? ` Cutoff: ${formatTimestamp(cutoffTimestamp)}.` : ""}
          </CardDescription>
          {staleError && <p className="text-xs text-red-600">{staleError}</p>}
        </CardHeader>
        <CardContent className="space-y-3">
          {staleLoading && <p className="text-sm text-neutral-600">Loading expired scrapes...</p>}
          {!staleLoading && staleOptions.length === 0 && (
            <p className="text-sm text-neutral-600">No expired scrapes found right now.</p>
          )}
          {!staleLoading && staleOptions.length > 0 && (
            <>
              <Label htmlFor="expired-select" className="text-xs uppercase tracking-wide text-neutral-600">
                Select funds to queue
              </Label>
              <div className="flex flex-col gap-2 sm:flex-row sm:items-start">
                <select
                  id="expired-select"
                  multiple
                  value={staleSelection}
                  onChange={handleSelectionChange}
                  className="min-h-[180px] w-full rounded-md border border-neutral-300 bg-white px-3 py-2 text-sm shadow-sm outline-none transition focus:border-neutral-900 focus:ring-2 focus:ring-neutral-900/10"
                >
                  {staleOptions.map((opt) => (
                    <option key={opt.value} value={opt.value} disabled={opt.disabled}>
                      {opt.label}
                      {opt.disabled ? " (queued)" : ""}
                    </option>
                  ))}
                </select>
                <div className="flex flex-col gap-2 sm:w-48">
                  <Button onClick={queueSelection} disabled={staleSelection.length === 0}>
                    Add to queue
                  </Button>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => setStaleSelection([])}
                    disabled={staleSelection.length === 0}
                  >
                    Clear selection
                  </Button>
                </div>
              </div>
            </>
          )}
        </CardContent>
      </Card>

      <Card className="relative overflow-hidden">
        <div className="absolute inset-x-0 top-0 h-1 bg-gradient-to-r from-neutral-900 via-orange-500 to-neutral-900" />
        <CardHeader className="pb-2">
          <CardTitle className="flex flex-wrap items-center justify-between gap-3">
            <span>Charity comission PDF</span>
            <div className="flex items-center gap-2">
              <Button
                variant="outline"
                size="sm"
                onClick={() => loadCharityPdfFunds(true)}
                disabled={charityPdfLoading}
              >
                {charityPdfLoading ? "Refreshing..." : "Refresh list"}
              </Button>
              {latestCharityPdfFunds.length > 0 && (
                <Badge variant="outline">{latestCharityPdfFunds.length} missing PDF</Badge>
              )}
            </div>
          </CardTitle>
          <CardDescription>
            Latest Charity Commission rows where the PDF was missing or failed to read.
          </CardDescription>
          {charityPdfError && <p className="text-xs text-red-600">{charityPdfError}</p>}
        </CardHeader>
        <CardContent className="space-y-3">
          {charityPdfLoading && <p className="text-sm text-neutral-600">Loading Charity Commission PDF retries...</p>}
          {!charityPdfLoading && charityPdfOptions.length === 0 && (
            <p className="text-sm text-neutral-600">No Charity Commission PDF retries found right now.</p>
          )}
          {!charityPdfLoading && charityPdfOptions.length > 0 && (
            <>
              <Label htmlFor="charity-pdf-select" className="text-xs uppercase tracking-wide text-neutral-600">
                Select funds to queue
              </Label>
              <div className="flex flex-col gap-2 sm:flex-row sm:items-start">
                <select
                  id="charity-pdf-select"
                  multiple
                  value={charityPdfSelection}
                  onChange={handleCharityPdfSelectionChange}
                  className="min-h-[180px] w-full rounded-md border border-neutral-300 bg-white px-3 py-2 text-sm shadow-sm outline-none transition focus:border-neutral-900 focus:ring-2 focus:ring-neutral-900/10"
                >
                  {charityPdfOptions.map((opt) => (
                    <option key={opt.value} value={opt.value} disabled={opt.disabled}>
                      {opt.label}
                      {opt.disabled ? " (queued)" : ""}
                    </option>
                  ))}
                </select>
                <div className="flex flex-col gap-2 sm:w-48">
                  <Button onClick={queueCharityPdfSelection} disabled={charityPdfSelection.length === 0}>
                    Add to queue
                  </Button>
                  <Button
                    variant="ghost"
                    size="sm"
                    onClick={() => setCharityPdfSelection([])}
                    disabled={charityPdfSelection.length === 0}
                  >
                    Clear selection
                  </Button>
                </div>
              </div>
            </>
          )}
        </CardContent>
      </Card>

      <Card className="overflow-hidden">
        <div className="h-1 bg-neutral-900" />
        <CardHeader className="pb-2">
          <CardTitle className="flex flex-wrap items-center justify-between gap-3">
            <span>Rescrape queue</span>
            <Badge variant="outline">{queuedUrls.length} queued</Badge>
          </CardTitle>
          <CardDescription>Queued funds will be re-scraped and appended as new rows.</CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          {queuedUrls.length === 0 ? (
            <p className="text-sm text-neutral-600">
              Nothing queued yet. Select expired scrapes or Charity Commission PDF retries above to add them.
            </p>
          ) : (
            <ul className="space-y-2">
              {queuedUrls.map((url, idx) => {
                const row = queueLookup.get(url);
                const title = row?.fund_name || row?.fund_url || url;
                const subtitle = row?.fund_url || url;
                return (
                  <li
                    key={url}
                    className="flex items-start justify-between gap-3 rounded-lg border border-neutral-200 bg-white px-3 py-2"
                  >
                    <div className="min-w-0">
                      <p className="text-[11px] uppercase tracking-wide text-neutral-500">#{idx + 1}</p>
                      <p className="truncate text-sm font-semibold text-neutral-900">{title}</p>
                      <p className="truncate text-xs text-neutral-500">{subtitle}</p>
                    </div>
                    <Button variant="ghost" size="sm" onClick={() => removeFromQueue(url)}>
                      Remove
                    </Button>
                  </li>
                );
              })}
            </ul>
          )}

          <div className="flex flex-wrap items-center justify-between gap-3">
            <Button variant="ghost" onClick={clearQueue} disabled={queuedUrls.length === 0 || isScraping}>
              Clear queue
            </Button>
            <Button onClick={startRescrape} disabled={queuedUrls.length === 0 || isScraping}>
              {isScraping ? "Starting..." : "Rescrape queued funds"}
            </Button>
          </div>

          {jobError && <p className="text-sm text-red-600">{jobError}</p>}

          {job && (
            <div className="rounded-xl border border-neutral-200 bg-neutral-50 p-4">
              {(() => {
                const progressValue = Math.max(0, Math.min(100, Number(job.progress_percent) || 0));
                const totalElapsed = formatSeconds(job.total_elapsed_seconds || 0);
                const currentElapsed = formatSeconds(job.current_elapsed_seconds || 0);
                return (
                  <>
                    <div className="flex flex-wrap items-center justify-between gap-3">
                      <div>
                        <p className="text-xs uppercase tracking-wide text-neutral-500">
                          Job {job.job_id.slice(0, 8)}
                        </p>
                        <p className="text-sm font-semibold text-neutral-900">
                          {job.done ? "Completed" : "Processing"} - {job.completed_urls || job.results.length}/
                          {job.total_urls || "?"} done
                        </p>
                      </div>
                      <div className="min-w-[200px]">
                        <Progress value={progressValue} />
                        <p className="mt-1 text-xs text-neutral-600">{progressValue}% complete</p>
                        <p className="text-[11px] text-neutral-500">Total elapsed: {totalElapsed}</p>
                      </div>
                      {job.done && (
                        <Button variant="ghost" size="sm" onClick={clearCompletedJob}>
                          Clear completed job
                        </Button>
                      )}
                    </div>
                    {job.current_url && (
                      <div className="mt-3 rounded-lg border border-neutral-200 bg-white p-3">
                        <p className="text-[11px] uppercase tracking-wide text-neutral-500">Currently scraping</p>
                        <p className="truncate text-sm font-semibold text-neutral-900">{job.current_url}</p>
                        <p className="text-xs text-neutral-600">Elapsed: {currentElapsed}</p>
                      </div>
                    )}
                    {job.errors.length > 0 && (
                      <div className="mt-3 space-y-1">
                        <p className="text-[11px] font-semibold uppercase tracking-wide text-red-600">Errors</p>
                        {job.errors.map((err) => (
                          <p key={err.url} className="text-sm text-red-600">
                            {err.url}: {err.message}
                          </p>
                        ))}
                      </div>
                    )}
                    {job.results.length > 0 && (
                      <div className="mt-4 space-y-2">
                        <p className="text-[11px] uppercase tracking-wide text-neutral-500">Latest results</p>
                        <ul className="space-y-2 text-sm text-neutral-800">
                          {job.results
                            .slice(-5)
                            .reverse()
                            .map((res) => (
                              <li
                                key={`${res.fund_url}-${res.fund_name}`}
                                className="flex items-start justify-between gap-3"
                              >
                                <div className="min-w-0">
                                  <p className="truncate font-semibold text-neutral-900">
                                    {res.fund_name || res.fund_url}
                                  </p>
                                  <p className="truncate text-xs text-neutral-500">{res.fund_url}</p>
                                </div>
                                <Badge variant="outline" className="whitespace-nowrap">
                                  {res.eligibility || "Pending"}
                                </Badge>
                              </li>
                            ))}
                        </ul>
                      </div>
                    )}
                    {job.url_timings && job.url_timings.length > 0 && (
                      <div className="mt-4 space-y-2">
                        <p className="text-[11px] uppercase tracking-wide text-neutral-500">Per-fund timing</p>
                        <ul className="space-y-2 text-sm text-neutral-800">
                          {job.url_timings
                            .slice(-5)
                            .reverse()
                            .map((t) => (
                              <li
                                key={`${t.url}-${t.finished_at || t.started_at || Math.random()}`}
                                className="flex items-center justify-between rounded-lg border border-neutral-200 bg-white px-3 py-2"
                              >
                                <div className="min-w-0">
                                  <p className="truncate font-semibold text-neutral-900">{t.url}</p>
                                  <p className="text-xs text-neutral-600">
                                    {t.error ? `Error: ${t.error}` : "Completed"}
                                  </p>
                                </div>
                                <Badge variant="muted" className="whitespace-nowrap">
                                  {formatSeconds(t.duration_seconds || 0)}
                                </Badge>
                              </li>
                            ))}
                        </ul>
                      </div>
                    )}
                  </>
                );
              })()}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
}

function normalizeUrlClient(url?: string): string {
  if (!url) return "";
  try {
    const parsed = new URL(url);
    const path = parsed.pathname.replace(/\/$/, "");
    return `${parsed.protocol}//${parsed.host}${path}${parsed.search}`;
  } catch {
    return url;
  }
}

function isCharityCommissionUrl(url: any): boolean {
  const normalized = normalizeUrlClient(typeof url === "string" ? url : "").toLowerCase();
  return normalized.includes("register-of-charities.charitycommission.gov.uk");
}

function asBoolean(value: any): boolean {
  if (value === true) return true;
  if (typeof value === "number") return value > 0;
  if (typeof value === "string") {
    return ["true", "1", "yes", "y"].includes(value.trim().toLowerCase());
  }
  return false;
}

function isCharityCommissionPdfCandidate(row: ResultRecord): boolean {
  if (!isCharityCommissionUrl(row?.fund_url)) return false;
  const pdfRead = asBoolean(row?.pdf_read);
  const pdfUrl = typeof row?.pdf_url === "string" ? row.pdf_url.trim() : "";
  return !pdfRead || !pdfUrl;
}

function getCharityPdfIssueLabel(row: ResultRecord): string {
  const pdfRead = asBoolean(row?.pdf_read);
  const pdfUrl = typeof row?.pdf_url === "string" ? row.pdf_url.trim() : "";
  if (!pdfUrl) return "missing PDF URL";
  if (!pdfRead) return "PDF fetch/read failed";
  return "PDF missing";
}

function parseTimestampMs(value: any): number | null {
  if (!value) return null;
  const direct = new Date(value);
  if (!Number.isNaN(direct.getTime())) return direct.getTime();
  if (typeof value === "string") {
    const normalized = value.trim().replace(" ", "T");
    const fallback = new Date(normalized);
    if (!Number.isNaN(fallback.getTime())) return fallback.getTime();
  }
  return null;
}

function compareTimestampValues(a: any, b: any): number {
  const aMs = parseTimestampMs(a);
  const bMs = parseTimestampMs(b);
  if (aMs === null && bMs === null) return 0;
  if (aMs === null) return -1;
  if (bMs === null) return 1;
  return aMs - bMs;
}

function formatAgeLabel(timestamp: any): string | null {
  const parsedMs = parseTimestampMs(timestamp);
  if (parsedMs === null) return null;
  const diffMs = Date.now() - parsedMs;
  const days = Math.floor(diffMs / (1000 * 60 * 60 * 24));
  if (days < 1) return "today";
  if (days < 30) return `${days}d ago`;
  const months = Math.floor(days / 30);
  return `${months}mo ago`;
}

function formatTimestamp(value: any) {
  const parsedMs = parseTimestampMs(value);
  if (parsedMs === null) return value ? String(value) : "-";
  const date = new Date(parsedMs);
  return date.toLocaleString();
}

function formatSeconds(totalSeconds: number): string {
  const s = Math.max(0, Math.floor(totalSeconds));
  const hours = Math.floor(s / 3600);
  const minutes = Math.floor((s % 3600) / 60);
  const seconds = s % 60;
  const parts = [
    hours > 0 ? `${hours}h` : null,
    minutes > 0 ? `${minutes}m` : null,
    `${seconds}s`,
  ].filter(Boolean);
  return parts.join(" ");
}
