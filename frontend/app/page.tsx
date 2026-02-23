import ScrapeForm from "../components/ScrapeForm";

export default function HomePage() {
  return (
    <div className="space-y-6">
      <header>
        <h1 className="text-3xl font-bold text-neutral-950">Automate your funding research</h1>

      </header>
      <ScrapeForm />
    </div>
  );
}
