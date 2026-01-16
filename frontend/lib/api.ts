const BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE_URL ||
  process.env.NEXT_PUBLIC_API_BASE ||
  "https://automated-funding-api--0000001.ambitioussand-ae029d29.eastus.azurecontainerapps.io";

type RequestOptions = RequestInit & {
  headers?: Record<string, string>;
};

async function request<T = any>(path: string, opts?: RequestOptions): Promise<T> {
  const res = await fetch(`${BASE_URL}${path}`, {
    ...opts,
    headers: { "Content-Type": "application/json", ...(opts?.headers || {}) },
  });
  if (!res.ok) {
    const text = await res.text();
    throw new Error(text || `Request failed with status ${res.status}`);
  }
  return res.json() as Promise<T>;
}

export const api = {
  results: () => request<{ results: any[] }>("/results/"),
  scrapeSingle: (fundUrl: string, fundName?: string) =>
    request("/scrape/single", { method: "POST", body: JSON.stringify({ fund_url: fundUrl, fund_name: fundName }) }),
  scrapeBatch: (fundUrls: string[], rescrapeUrls?: string[]) =>
    request("/scrape/batch", {
      method: "POST",
      body: JSON.stringify({ fund_urls: fundUrls, rescrape_urls: rescrapeUrls || [] }),
    }),
  jobStatus: (jobId: string) => request(`/scrape/jobs/${jobId}`),
  prepareUrls: (fundUrls: string[]) =>
    request("/scrape/prepare", { method: "POST", body: JSON.stringify({ fund_urls: fundUrls }) }),
  refreshResults: () => request("/results/refresh", { method: "POST" }),
  updateOpenAIKey: (apiKey: string) =>
    request("/settings/openai", { method: "POST", body: JSON.stringify({ openai_api_key: apiKey }) }),
};
