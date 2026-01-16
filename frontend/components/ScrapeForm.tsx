"use client";

import type { ChangeEvent } from "react";
import { useEffect, useMemo, useRef, useState } from "react";
import { api } from "../lib/api";
import { clearCache, readCache, writeCache } from "../lib/storage";
import { Badge } from "./ui/badge";
import { Button } from "./ui/button";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "./ui/card";
import { Input } from "./ui/input";
import { Label } from "./ui/label";
import { Progress } from "./ui/progress";
import { Textarea } from "./ui/textarea";

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

type PrepSummary = {
  added: string[];
  alreadyProcessed: string[];
  duplicatesInPayload: string[];
  normalizedMap: Record<string, string>;
};

type QueueStats = {
  uniqueDomains: number;
  totalQueued: number;
};

type ScrapeCache = {
  manualInput: string;
  stagedUrls: string[];
  prepSummary: PrepSummary | null;
  queueStats: QueueStats;
  job: JobStatus | null;
  rescrapeUrls: string[];
  rescrapeSelection: string[];
};

const extractUrls = (text: string) => {
  const matches = text.match(/https?:\/\/[^\s,"'>)]+/gi) || [];
  return matches.map((u) => u.trim());
};

const SCRAPE_CACHE_KEY = "scrape_form_cache_v1";
const RESULTS_FORCE_REFRESH_KEY = "results_force_refresh_v1";

export default function ScrapeForm() {
  const [manualInput, setManualInput] = useState("");
  const [stagedUrls, setStagedUrls] = useState<string[]>([]);
  const [prepSummary, setPrepSummary] = useState<PrepSummary | null>(null);
  const [prepError, setPrepError] = useState<string | null>(null);
  const [jobError, setJobError] = useState<string | null>(null);
  const [job, setJob] = useState<JobStatus | null>(null);
  const [isPreparing, setIsPreparing] = useState(false);
  const [isScraping, setIsScraping] = useState(false);
  const [rescrapeUrls, setRescrapeUrls] = useState<string[]>([]);
  const [rescrapeSelection, setRescrapeSelection] = useState<string[]>([]);
  const [isRefreshingSheet, setIsRefreshingSheet] = useState(false);
  const [sheetRefreshMessage, setSheetRefreshMessage] = useState<string | null>(null);
  const [sheetRefreshError, setSheetRefreshError] = useState<string | null>(null);
  const [queueStats, setQueueStats] = useState<QueueStats>({ uniqueDomains: 0, totalQueued: 0 });
  const [hydratedCache, setHydratedCache] = useState(false);
  const lastCompletedJobId = useRef<string | null>(null);

  const clearManualInput = () => {
    setManualInput("");
    setPrepError(null);
    setPrepSummary(null);
    setRescrapeSelection([]);
  };

  const resetAll = () => {
    setManualInput("");
    setStagedUrls([]);
    setPrepSummary(null);
    setPrepError(null);
    setJobError(null);
    setJob(null);
    setRescrapeUrls([]);
    setRescrapeSelection([]);
    setSheetRefreshMessage(null);
    setSheetRefreshError(null);
    setQueueStats({ uniqueDomains: 0, totalQueued: 0 });
    lastCompletedJobId.current = null;
    clearCache(SCRAPE_CACHE_KEY);
    clearCache(RESULTS_FORCE_REFRESH_KEY);
  };

  const detectedManualUrls = useMemo(() => extractUrls(manualInput), [manualInput]);

  useEffect(() => {
    const cached = readCache<ScrapeCache>(SCRAPE_CACHE_KEY)?.value;
    if (!cached) {
      setHydratedCache(true);
      return;
    }
    const cachedSummary = cached.prepSummary
      ? { ...cached.prepSummary, normalizedMap: cached.prepSummary.normalizedMap || {} }
      : null;
    setManualInput(cached.manualInput || "");
    setStagedUrls(cached.stagedUrls || []);
    setPrepSummary(cachedSummary);
    setRescrapeUrls(cached.rescrapeUrls || []);
    setRescrapeSelection(cached.rescrapeSelection || []);
    setQueueStats(cached.queueStats || calcQueueStats(cached.stagedUrls || [], []));
    setJob(cached.job || null);
    setHydratedCache(true);
  }, []);

  useEffect(() => {
    if (!hydratedCache) return;
    const payload: ScrapeCache = {
      manualInput,
      stagedUrls,
      prepSummary,
      queueStats,
      job,
      rescrapeUrls,
      rescrapeSelection,
    };
    writeCache(SCRAPE_CACHE_KEY, payload);
  }, [hydratedCache, manualInput, stagedUrls, prepSummary, queueStats, job, rescrapeUrls, rescrapeSelection]);

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
      } catch (err: any) {
        console.error(err);
      }
    }, 4000);
    return () => clearInterval(interval);
  }, [job]);

  const prepareAndStage = async (urls: string[]) => {
    const candidates = Array.from(new Set(urls.map((u) => u.trim()))).filter(Boolean);
    if (candidates.length === 0) {
      setPrepError("No URLs detected to stage.");
      return;
    }

    setPrepError(null);
    setIsPreparing(true);
    try {
      const res = await api.prepareUrls(candidates);
      const addedNow: string[] = [];
      setStagedUrls((prev) => {
        const next = [...prev];
        res.to_scrape.forEach((u: string) => {
          if (!next.includes(u)) {
            next.push(u);
            addedNow.push(u);
          }
        });
        setQueueStats(calcQueueStats(next, []));
        return next;
      });
      setPrepSummary({
        added: addedNow,
        alreadyProcessed: res.already_processed || [],
        duplicatesInPayload: res.duplicates_in_payload || [],
        normalizedMap: res.normalized_map || {},
      });
      setRescrapeSelection([]);
    } catch (err: any) {
      setPrepError(err.message || "Could not prepare URLs.");
    } finally {
      setIsPreparing(false);
    }
  };

  const handleManualStage = async () => {
    await prepareAndStage(detectedManualUrls);
  };

  const handleCsvUpload = async (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;
    try {
      const text = await file.text();
      const urls = extractUrls(text);
      await prepareAndStage(urls);
    } catch (err: any) {
      setPrepError(err.message || "Could not read CSV file.");
    } finally {
      // reset so the same file can be re-selected if needed
      event.target.value = "";
    }
  };

  const removeFromQueue = (url: string) => {
    setStagedUrls((prev) => {
      const next = prev.filter((u) => u !== url);
      setQueueStats(calcQueueStats(next, []));
      return next;
    });
    setRescrapeUrls((prev) => prev.filter((u) => u !== url));
  };

  const clearQueue = () => {
    setStagedUrls([]);
    setRescrapeUrls([]);
    setRescrapeSelection([]);
    setQueueStats({ uniqueDomains: 0, totalQueued: 0 });
    setPrepSummary(null);
    setPrepError(null);
  };

  const handleRescrapeSelectionChange = (event: ChangeEvent<HTMLSelectElement>) => {
    const selected = Array.from(event.target.selectedOptions).map((opt) => opt.value);
    setRescrapeSelection(selected);
  };

  const queueRescrapeSelection = () => {
    if (!prepSummary || rescrapeSelection.length === 0) return;
    const normalizedSelection = rescrapeSelection
      .map((url) => prepSummary.normalizedMap[url] || url)
      .filter(Boolean);
    setStagedUrls((prev) => {
      const next = [...prev];
      normalizedSelection.forEach((url) => {
        if (!next.includes(url)) next.push(url);
      });
      setQueueStats(calcQueueStats(next, []));
      return next;
    });
    setRescrapeUrls((prev) => {
      const next = [...prev];
      normalizedSelection.forEach((url) => {
        if (!next.includes(url)) next.push(url);
      });
      return next;
    });
    setRescrapeSelection([]);
  };

  const refreshSheetConnection = async () => {
    setIsRefreshingSheet(true);
    setSheetRefreshMessage(null);
    setSheetRefreshError(null);
    try {
      const res = await api.refreshResults();
      setSheetRefreshMessage(`Sheet refreshed (${res.total_results || 0} rows).`);
    } catch (err: any) {
      setSheetRefreshError(err.message || "Failed to refresh the sheet connection.");
    } finally {
      setIsRefreshingSheet(false);
    }
  };

  const startScrape = async () => {
    if (stagedUrls.length === 0) return;
    setIsScraping(true);
    setJobError(null);
    try {
      const payload = await api.scrapeBatch(stagedUrls, rescrapeUrls);
      const status = await api.jobStatus(payload.job_id);
      setJob(status);
      setStagedUrls([]);
      setRescrapeUrls([]);
      setRescrapeSelection([]);
      setPrepSummary({
        added: payload.to_scrape || [],
        alreadyProcessed: payload.already_processed || [],
        duplicatesInPayload: payload.duplicates_in_payload || [],
        normalizedMap: {},
      });
      setQueueStats({ uniqueDomains: 0, totalQueued: 0 });
    } catch (err: any) {
      setJobError(err.message || "Failed to start scrape job.");
    } finally {
      setIsScraping(false);
    }
  };

  const clearCompletedJob = () => {
    setJob(null);
    setJobError(null);
    lastCompletedJobId.current = null;
    clearCache(RESULTS_FORCE_REFRESH_KEY);
  };

  return (
    <div className="space-y-8">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <p className="text-xs uppercase tracking-wide text-neutral-500">Scrape controls</p>
          <p className="text-sm text-neutral-700">Paste URLs, upload CSVs, queue them, and monitor jobs.</p>
        </div>
        <div className="flex items-center gap-2">
          <Button variant="outline" size="sm" onClick={refreshSheetConnection} disabled={isRefreshingSheet}>
            {isRefreshingSheet ? "Refreshing sheet..." : "Refresh sheet"}
          </Button>
          <Button variant="ghost" size="sm" onClick={clearManualInput}>
            Clear manual text
          </Button>
          <Button variant="outline" size="sm" onClick={resetAll}>
            Reset all fields
          </Button>
        </div>
      </div>
      {sheetRefreshMessage && <p className="text-xs text-neutral-500">{sheetRefreshMessage}</p>}
      {sheetRefreshError && <p className="text-xs text-red-600">{sheetRefreshError}</p>}

      <div className="grid gap-6 lg:grid-cols-2">
        <Card className="relative overflow-hidden">
          <div className="absolute inset-x-0 top-0 h-1 bg-gradient-to-r from-neutral-900 via-orange-500 to-neutral-900" />
          <CardHeader className="pb-2">
            <div className="flex items-start justify-between gap-3">
              <div className="space-y-1">
                <Badge variant="accent" className="w-fit">
                  Manual entry
                </Badge>
                <CardTitle>Paste one or many fund URLs</CardTitle>
                <CardDescription>
                  Enter as many URLs as you like — we will normalize and de-duplicate before scraping.
                </CardDescription>
              </div>
              <Button variant="ghost" size="sm" onClick={clearManualInput}>
                Clear
              </Button>
            </div>
          </CardHeader>
          <CardContent className="space-y-4">
            <Textarea
              value={manualInput}
              onChange={(e) => setManualInput(e.target.value)}
              placeholder={"https://example.org/grant-1\nhttps://example.org/grant-2"}
              className="min-h-[170px]"
            />
            <div className="flex flex-wrap items-center justify-between gap-3 text-sm text-neutral-600">
              <span>{detectedManualUrls.length} URL(s) detected</span>
              <Button variant="outline" onClick={handleManualStage} disabled={detectedManualUrls.length === 0 || isPreparing}>
                {isPreparing ? "Staging..." : "Add to queue"}
              </Button>
            </div>
          </CardContent>
        </Card>

        <Card className="relative overflow-hidden">
          <div className="absolute inset-x-0 top-0 h-1 bg-neutral-900" />
          <CardHeader className="pb-2">
            <Badge variant="outline" className="w-fit">
              CSV import
            </Badge>
            <CardTitle>Upload a CSV of potential funders</CardTitle>
            <CardDescription>We will scan the file for http(s) URLs, skip ones already in results, and stage the rest.</CardDescription>
          </CardHeader>
          <CardContent className="space-y-3">
            <div className="space-y-2">
              <Label htmlFor="csv-upload">CSV file</Label>
              <Input id="csv-upload" type="file" accept=".csv" onChange={handleCsvUpload} />
            </div>
            <p className="text-sm text-neutral-600">
              Columns are auto-detected; any link found in the file will be added to the queue if it is not already scraped.
            </p>
          </CardContent>
        </Card>
      </div>

      <Card className="overflow-hidden">
        <div className="h-1 bg-gradient-to-r from-neutral-900 via-orange-500 to-neutral-900" />
        <CardHeader className="pb-2">
          <CardTitle className="flex items-center justify-between">
            <span>Funds about to scrape</span>
            <div className="flex items-center gap-2">
              <Badge variant="outline">{stagedUrls.length} queued</Badge>
              {rescrapeUrls.length > 0 && <Badge variant="muted">{rescrapeUrls.length} rescrape</Badge>}
            </div>
          </CardTitle>
          <CardDescription>
            URLs must pass the pre-check (no duplicates). Already-processed funds can be re-queued below for rescraping.
          </CardDescription>
        </CardHeader>
        <CardContent className="space-y-4">
          {prepSummary && (
            <div className="rounded-lg border border-neutral-200 bg-neutral-50 p-3 text-sm text-neutral-800">
              <p className="font-semibold text-neutral-900">Pre-check summary</p>
              <div className="mt-2 grid gap-2 sm:grid-cols-4">
                <div>
                  <p className="text-xs uppercase tracking-wide text-neutral-500">Added to queue</p>
                  <p className="text-lg font-semibold text-neutral-900">{prepSummary.added.length}</p>
                </div>
                <div>
                  <p className="text-xs uppercase tracking-wide text-neutral-500">Already in results</p>
                  <p className="text-lg font-semibold text-neutral-900">{prepSummary.alreadyProcessed.length}</p>
                </div>
                <div>
                  <p className="text-xs uppercase tracking-wide text-neutral-500">Duplicates removed</p>
                  <p className="text-lg font-semibold text-neutral-900">{prepSummary.duplicatesInPayload.length}</p>
                </div>
                <div>
                  <p className="text-xs uppercase tracking-wide text-neutral-500">Unique domains queued</p>
                  <p className="text-lg font-semibold text-neutral-900">{queueStats.uniqueDomains}</p>
                </div>
              </div>
              {(prepSummary.alreadyProcessed.length > 0 || prepSummary.duplicatesInPayload.length > 0) && (
                <p className="mt-2 text-xs text-neutral-600">
                  Duplicate entries are ignored. Already-processed URLs can be queued again below if you want to re-scrape them.
                </p>
              )}
              {prepSummary.alreadyProcessed.length > 0 && (
                <div className="mt-3 space-y-2">
                  <Label htmlFor="rescrape-select" className="text-xs uppercase tracking-wide text-neutral-600">
                    Re-scrape already processed
                  </Label>
                  <div className="flex flex-col gap-2 sm:flex-row sm:items-start">
                    <select
                      id="rescrape-select"
                      multiple
                      value={rescrapeSelection}
                      onChange={handleRescrapeSelectionChange}
                      className="min-h-[120px] w-full rounded-md border border-neutral-300 bg-white px-3 py-2 text-sm shadow-sm outline-none focus:border-neutral-900 focus:ring-2 focus:ring-neutral-900/10"
                    >
                      {prepSummary.alreadyProcessed.map((url) => {
                        const normalized = prepSummary.normalizedMap[url] || url;
                        const isQueued = stagedUrls.includes(normalized);
                        return (
                          <option key={url} value={url} disabled={isQueued}>
                            {url}
                            {isQueued ? " (queued)" : ""}
                          </option>
                        );
                      })}
                    </select>
                    <Button
                      variant="outline"
                      size="sm"
                      onClick={queueRescrapeSelection}
                      disabled={rescrapeSelection.length === 0}
                    >
                      Queue rescrape
                    </Button>
                  </div>
                  <p className="text-xs text-neutral-600">
                    Selected URLs will be scraped again and appended as new rows in the sheet.
                  </p>
                </div>
              )}
            </div>
          )}

          {prepError && <p className="text-sm text-red-600">{prepError}</p>}

          <div className="space-y-2 rounded-xl border border-dashed border-neutral-300 p-4">
            {stagedUrls.length === 0 ? (
              <p className="text-sm text-neutral-600">Nothing queued yet. Paste URLs or upload a CSV to start.</p>
            ) : (
              <ul className="space-y-2">
                {stagedUrls.map((url, idx) => {
                  const isRescrape = rescrapeUrls.includes(url);
                  return (
                    <li
                      key={url}
                      className="flex items-start justify-between gap-3 rounded-lg border border-neutral-200 bg-white px-3 py-2"
                    >
                      <div className="min-w-0">
                        <p className="text-[11px] uppercase tracking-wide text-neutral-500">#{idx + 1}</p>
                        <p className="truncate text-sm font-semibold text-neutral-900">{url}</p>
                        <p className="text-xs text-neutral-600">Domain: {safeDomain(url)}</p>
                        {isRescrape && (
                          <Badge variant="muted" className="mt-2 w-fit">
                            Rescrape
                          </Badge>
                        )}
                      </div>
                      <Button variant="ghost" size="sm" onClick={() => removeFromQueue(url)}>
                        Remove
                      </Button>
                    </li>
                  );
                })}
              </ul>
            )}
          </div>

          <div className="flex flex-wrap items-center justify-between gap-3">
            <div className="space-x-2">
              <Button variant="ghost" disabled={stagedUrls.length === 0 || isScraping} onClick={clearQueue}>
                Clear queue
              </Button>
              <Badge variant="outline" className="align-middle">
                {stagedUrls.length} ready to scrape
              </Badge>
              <Badge variant="muted" className="align-middle">
                {queueStats.uniqueDomains} unique domains
              </Badge>
            </div>
            <Button onClick={startScrape} disabled={stagedUrls.length === 0 || isScraping}>
              {isScraping ? "Starting..." : "Scrape queued funds"}
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
                          {job.done ? "Completed" : "Processing"} • {job.completed_urls || job.results.length}/
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

function safeDomain(url: string): string {
  try {
    return new URL(url).hostname || "unknown";
  } catch {
    return "unknown";
  }
}

function calcQueueStats(existing: string[], added: string[]): QueueStats {
  const urls = [...existing, ...added];
  const domains = new Set<string>();
  urls.forEach((u) => {
    const d = safeDomain(u);
    if (d !== "unknown") domains.add(d);
  });
  return { uniqueDomains: domains.size, totalQueued: urls.length };
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
