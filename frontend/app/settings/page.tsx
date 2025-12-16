"use client";

import { useState } from "react";

export default function SettingsPage() {
  const [apiKey, setApiKey] = useState("");
  const [sheetId, setSheetId] = useState("");
  const [status, setStatus] = useState<string | null>(null);

  const handleSave = () => {
    setStatus("Settings saved locally. Configure environment variables in Azure for production.");
    localStorage.setItem("ellenor_api_key", apiKey);
    localStorage.setItem("ellenor_sheet_id", sheetId);
  };

  return (
    <div className="space-y-6">
      <header>
        <p className="text-sm uppercase tracking-wide text-slate-500">Settings</p>
        <h1 className="text-3xl font-bold text-slate-900">API & storage configuration</h1>
        <p className="text-sm text-slate-600">Values set here are only stored in your browser for preview. Use platform secrets for production.</p>
      </header>
      <section className="rounded-2xl bg-white p-6 shadow">
        <div className="grid gap-4 sm:grid-cols-2">
          <div>
            <label className="text-sm font-medium">OpenAI API Key</label>
            <input type="password" value={apiKey} onChange={(e) => setApiKey(e.target.value)} placeholder="sk-..." />
          </div>
          <div>
            <label className="text-sm font-medium">Google Sheet ID</label>
            <input value={sheetId} onChange={(e) => setSheetId(e.target.value)} placeholder="1abcdEf..." />
          </div>
        </div>
        <button className="mt-4" onClick={handleSave}>
          Save to browser
        </button>
        {status && <p className="mt-3 text-sm text-slate-600">{status}</p>}
      </section>
      <section className="rounded-2xl bg-slate-900 p-6 text-white">
        <h2 className="text-xl font-semibold">Deploying to Azure</h2>
        <ol className="mt-4 list-decimal space-y-2 pl-6 text-sm text-slate-200">
          <li>Set <code>OPENAI_API_KEY</code>, <code>GOOGLE_SHEET_ID</code>, and <code>GCP_SERVICE_ACCOUNT_JSON</code> in Azure Container Apps.</li>
          <li>Expose the FastAPI host via Azure Static Web Apps custom domain.</li>
          <li>Configure <code>NEXT_PUBLIC_API_BASE_URL</code> for the frontend environment.</li>
        </ol>
      </section>
    </div>
  );
}
