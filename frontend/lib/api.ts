export const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE_URL ||
  process.env.NEXT_PUBLIC_API_BASE ||
  "https://automated-funding-api--0000001.ambitioussand-ae029d29.eastus.azurecontainerapps.io";

type RequestOptions = RequestInit & {
  headers?: Record<string, string>;
};

async function request<T = any>(path: string, opts?: RequestOptions): Promise<T> {
  const { headers, ...rest } = opts || {};
  const res = await fetch(`${API_BASE_URL}${path}`, {
    cache: "no-store",
    ...rest,
    headers: { "Content-Type": "application/json", ...(headers || {}) },
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `Request failed with status ${res.status}`);
  }
  return res.json() as Promise<T>;
}

export const api = {
  results: (opts?: { forceRefresh?: boolean }) => {
    const params = new URLSearchParams();
    if (opts?.forceRefresh) params.set("force_refresh", "true");
    const query = params.toString();
    return request<{ results: any[] }>(`/results/${query ? `?${query}` : ""}`);
  },
  staleResults: (months = 3, opts?: { forceRefresh?: boolean }) => {
    const params = new URLSearchParams({ months: String(months) });
    if (opts?.forceRefresh) params.set("force_refresh", "true");
    return request<{ results: any[]; months: number; cutoff_timestamp?: string }>(`/results/stale?${params}`);
  },
  scrapeSingle: (fundUrl: string, fundName?: string) =>
    request("/scrape/single", { method: "POST", body: JSON.stringify({ fund_url: fundUrl, fund_name: fundName }) }),
  scrapeBatch: (
    fundUrls: string[],
    rescrapeUrls?: string[],
    opts?: { rescrapeScope?: "stale" | "any" },
  ) =>
    request("/scrape/batch", {
      method: "POST",
      body: JSON.stringify({
        fund_urls: fundUrls,
        rescrape_urls: rescrapeUrls || [],
        rescrape_scope: opts?.rescrapeScope || "stale",
      }),
    }),
  jobStatus: (jobId: string) => request(`/scrape/jobs/${jobId}`),
  prepareUrls: (fundUrls: string[]) =>
    request("/scrape/prepare", { method: "POST", body: JSON.stringify({ fund_urls: fundUrls }) }),
  refreshResults: () => request("/results/refresh", { method: "POST" }),
  updateOpenAIKey: (apiKey: string) =>
    request("/settings/openai", { method: "POST", body: JSON.stringify({ openai_api_key: apiKey }) }),
};
