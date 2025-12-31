import ScrapeForm from "../components/ScrapeForm";

export default function HomePage() {
  return (
    <div className="space-y-6">
      <header>
        <p className="text-sm uppercase tracking-wide text-neutral-500">Scrape & Analyze</p>
        <h1 className="text-3xl font-bold text-neutral-950">Automate your funding research</h1>
        <p className="mt-2 text-base text-neutral-600">
          Paste URLs or batch process CSV exports. We'll crawl relevant pages and summarize eligibility with GPT-4.
        </p>
      </header>
      <ScrapeForm />
    </div>
  );
}
