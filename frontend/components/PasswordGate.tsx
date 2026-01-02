"use client";

import { useEffect, useMemo, useState } from "react";
import { Input } from "./ui/input";
import { Button } from "./ui/button";

const STORAGE_KEY = "app_pass_v1";

function getExpectedPassword() {
  return process.env.NEXT_PUBLIC_APP_PASSWORD?.trim() || "";
}

export default function PasswordGate({ children }: { children: React.ReactNode }) {
  const expected = useMemo(() => getExpectedPassword(), []);
  const [unlocked, setUnlocked] = useState(false);
  const [attempt, setAttempt] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [hydrated, setHydrated] = useState(false);

  useEffect(() => {
    if (!expected) {
      setUnlocked(true);
      setHydrated(true);
      return;
    }
    try {
      const cached = window.localStorage.getItem(STORAGE_KEY);
      if (cached && cached === expected) {
        setUnlocked(true);
      }
    } catch {
      // ignore read errors
    } finally {
      setHydrated(true);
    }
  }, [expected]);

  const handleSubmit = () => {
    if (!expected) {
      setUnlocked(true);
      return;
    }
    if (attempt === expected) {
      setUnlocked(true);
      setError(null);
      try {
        window.localStorage.setItem(STORAGE_KEY, expected);
      } catch {
        // ignore write errors
      }
    } else {
      setError("Incorrect password. Please try again.");
    }
  };

  const handleSignOut = () => {
    setUnlocked(false);
    setAttempt("");
    try {
      window.localStorage.removeItem(STORAGE_KEY);
    } catch {
      // ignore
    }
  };

  if (!hydrated) {
    return <div className="flex min-h-screen items-center justify-center bg-slate-50 text-slate-600">Loading…</div>;
  }

  if (unlocked) {
    return (
      <>
        <div className="fixed right-4 top-4 z-20">
          <Button variant="outline" size="sm" onClick={handleSignOut}>
            Lock
          </Button>
        </div>
        {children}
      </>
    );
  }

  return (
    <div className="flex min-h-screen items-center justify-center bg-slate-900 px-4 text-white">
      <div className="w-full max-w-md space-y-4 rounded-2xl bg-slate-800 p-6 shadow-2xl ring-1 ring-white/10">
        <div>
          <p className="text-sm uppercase tracking-wide text-slate-300">Access required</p>
          <h1 className="text-2xl font-semibold">Enter the app password</h1>
          <p className="text-sm text-slate-300">
            Set by the environment variable <code className="font-mono">NEXT_PUBLIC_APP_PASSWORD</code>. Update it in
            Azure secrets or your local <code className="font-mono">.env.local</code>.
          </p>
        </div>
        <div className="space-y-2">
          <label className="text-xs uppercase tracking-wide text-slate-400" htmlFor="app-password">
            Password
          </label>
          <Input
            id="app-password"
            type="password"
            value={attempt}
            onChange={(e) => setAttempt(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter") handleSubmit();
            }}
            className="bg-slate-700/60 text-white placeholder:text-slate-400"
          />
          {error && <p className="text-sm text-red-300">{error}</p>}
        </div>
        <Button className="w-full bg-white text-slate-900 hover:bg-slate-200" onClick={handleSubmit}>
          Unlock
        </Button>
      </div>
    </div>
  );
}
