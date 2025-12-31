"use client";

import { useEffect, useState } from "react";
import { api } from "../../lib/api";
import { Button } from "../../components/ui/button";
import { Card, CardContent, CardHeader, CardTitle } from "../../components/ui/card";
import { Input } from "../../components/ui/input";
import { Label } from "../../components/ui/label";

const LOCAL_OPENAI_KEY = "ellenor_openai_key";

export default function SettingsPage() {
  const [apiKey, setApiKey] = useState("");
  const [sheetId] = useState("Managed on server");
  const [status, setStatus] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  useEffect(() => {
    const cached = localStorage.getItem(LOCAL_OPENAI_KEY);
    if (cached) setApiKey(cached);
  }, []);

  const handleSave = async () => {
    setStatus(null);
    setError(null);
    setSaving(true);
    try {
      await api.updateOpenAIKey(apiKey);
      localStorage.setItem(LOCAL_OPENAI_KEY, apiKey);
      setStatus("OpenAI key applied to the API for this session and saved in your browser.");
    } catch (err: any) {
      setError(err?.message || "Could not update OpenAI key.");
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="space-y-6">
      <header>
        <p className="text-sm uppercase tracking-wide text-neutral-500">Settings</p>
        <h1 className="text-3xl font-bold text-neutral-950">API & storage configuration</h1>
        <p className="text-sm text-neutral-600">
          Google Sheets credentials stay on the server. Use this page to change the OpenAI API key without touching Azure
          secrets.
        </p>
      </header>
      <Card>
        <CardHeader>
          <CardTitle>OpenAI runtime key</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          <div className="grid gap-4 sm:grid-cols-2">
            <div className="space-y-2">
              <Label htmlFor="api-key">OpenAI API Key</Label>
              <Input
                id="api-key"
                type="password"
                value={apiKey}
                onChange={(e) => setApiKey(e.target.value)}
                placeholder="sk-..."
                autoComplete="off"
              />
              <p className="text-xs text-neutral-500">
                Stored locally in this browser and pushed to the API for the current session only.
              </p>
            </div>
            <div className="space-y-2">
              <Label htmlFor="sheet-id">Google Sheet ID</Label>
              <Input id="sheet-id" value={sheetId} disabled className="text-neutral-500" />
              <p className="text-xs text-neutral-500">Managed server-side with the service account and not editable here.</p>
            </div>
          </div>
          <Button className="mt-2" onClick={handleSave} disabled={saving || !apiKey.trim()}>
            {saving ? "Saving..." : "Apply to API"}
          </Button>
          {status && <p className="text-sm text-neutral-600">{status}</p>}
          {error && <p className="text-sm text-red-600">{error}</p>}
        </CardContent>
      </Card>
    </div>
  );
}
