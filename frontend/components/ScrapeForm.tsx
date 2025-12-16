"use client";

import { useEffect, useMemo, useState } from "react";
import { api } from "../lib/api";

type JobStatus = {
  job_id: string;
  done: boolean;
  progress_percent: number;
  results: any[];
  errors: { url: string; message: string }[];
};

export default function ScrapeForm() {
  const [fundUrl, setFundUrl] = useState("");
  const [fundName, setFundName] = useState("");
  const [bulkInput, setBulkInput] = useState("");
  const [job, setJob] = useState<JobStatus | null>(null);
  const [singleResult, setSingleResult] = useState<any | null>(null);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const bulkUrls = useMemo(
    () => bulkInput.split(/\s+/).map((u) => u.trim()).filter(Boolean),
    [bulkInput]
  );

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

  const handleSingleSubmit = async () => {
    setIsSubmitting(true);
    setError(null);
    try {
      const res = await api.scrapeSingle(fundUrl, fundName || undefined);
      setSingleResult(res);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setIsSubmitting(false);
    }
  };

  const handleBatchSubmit = async () => {
    setIsSubmitting(true);
    setError(null);
    try {
      const payload = await api.scrapeBatch(bulkUrls);
      const status = await api.jobStatus(payload.job_id);
      setJob(status);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="space-y-10">
      <section className="rounded-2xl bg-white p-6 shadow">
        <h2 className="text-xl font-semibold">Scrape a single fund</h2>
        <p className="text-sm text-slate-500">Runs immediate scrape + LLM extraction.</p>
        <div className="mt-4 grid gap-4 md:grid-cols-2">
          <div>
            <label className="text-sm font-medium">Fund URL</label>
            <input value={fundUrl} onChange={(e) => setFundUrl(e.target.value)} placeholder="https://example.org/grants" />
          </div>
          <div>
            <label className="text-sm font-medium">Optional friendly name</label>
            <input value={fundName} onChange={(e) => setFundName(e.target.value)} placeholder="Example Community Grants" />
          </div>
        </div>
        <button className="mt-4" onClick={handleSingleSubmit} disabled={!fundUrl || isSubmitting}>
          {isSubmitting ? "Running..." : "Scrape now"}
        </button>
        {error && <p className="mt-2 text-sm text-red-600">{error}</p>}
        {singleResult && (
          <div className="mt-6 rounded-lg border border-slate-200 bg-slate-50 p-4">
            <h3 className="text-lg font-semibold">Latest result</h3>
            <p className="text-sm text-slate-600">{singleResult.fund_name}</p>
            <div className="mt-2 grid gap-3 md:grid-cols-2">
              <div>
                <p className="text-xs uppercase text-slate-500">Eligibility</p>
                <p className="text-base font-medium">{singleResult.eligibility || "Unknown"}</p>
              </div>
              <div>
                <p className="text-xs uppercase text-slate-500">Pages scraped</p>
                <p className="text-base font-medium">{singleResult.pages_scraped ?? 0}</p>
              </div>
            </div>
            {singleResult.error && <p className="mt-3 text-sm text-red-600">Error: {singleResult.error}</p>}
          </div>
        )}
      </section>

      <section className="rounded-2xl bg-white p-6 shadow">
        <h2 className="text-xl font-semibold">Bulk discovery</h2>
        <p className="text-sm text-slate-500">Queue multiple URLs. Progress updates every few seconds.</p>
        <textarea
          className="mt-4 min-h-[140px] w-full"
          value={bulkInput}
          onChange={(e) => setBulkInput(e.target.value)}
          placeholder="https://example.org/grant-1\nhttps://example.org/grant-2"
        />
        <div className="mt-3 flex items-center justify-between text-sm text-slate-500">
          <span>{bulkUrls.length} URLs detected</span>
          <button onClick={handleBatchSubmit} disabled={bulkUrls.length === 0 || isSubmitting}>
            {isSubmitting ? "Queuing..." : "Start batch scrape"}
          </button>
        </div>
        {job && (
          <div className="mt-6 rounded-lg border border-slate-200 p-4">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-semibold">Job {job.job_id.slice(0, 8)}</p>
                <p className="text-xs text-slate-500">
                  {job.done ? "Completed" : "Processing"} · {job.progress_percent}%
                </p>
              </div>
              <div className="h-3 w-40 overflow-hidden rounded-full bg-slate-100">
                <div className="h-full bg-brand" style={{ width: `${job.progress_percent}%` }} />
              </div>
            </div>
            {job.errors.length > 0 && (
              <div className="mt-4 space-y-2">
                <p className="text-sm font-semibold text-red-600">Errors</p>
                {job.errors.map((err) => (
                  <p key={err.url} className="text-sm text-red-600">
                    {err.url}: {err.message}
                  </p>
                ))}
              </div>
            )}
            {job.results.length > 0 && (
              <div className="mt-4">
                <p className="text-sm font-semibold">Latest results</p>
                <ul className="mt-2 space-y-1 text-sm text-slate-600">
                  {job.results.slice(-5).map((res) => (
                    <li key={res.fund_url}>{res.fund_name || res.fund_url} — {res.eligibility || "Unknown"}</li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        )}
      </section>
    </div>
  );
}
