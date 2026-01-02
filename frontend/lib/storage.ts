type CachePayload<T> = {
  value: T;
  timestamp: number;
};

const isBrowser = typeof window !== "undefined";

export function readCache<T>(key: string): CachePayload<T> | null {
  if (!isBrowser) return null;
  try {
    const raw = window.localStorage.getItem(key);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object" || !("value" in parsed)) return null;
    return {
      value: (parsed as any).value as T,
      timestamp: typeof (parsed as any).timestamp === "number" ? (parsed as any).timestamp : Date.now(),
    };
  } catch {
    return null;
  }
}

export function writeCache<T>(key: string, value: T) {
  if (!isBrowser) return;
  const payload: CachePayload<T> = { value, timestamp: Date.now() };
  try {
    window.localStorage.setItem(key, JSON.stringify(payload));
  } catch {
    // Best-effort cache; ignore storage errors (quota, etc).
  }
}

export function clearCache(key: string) {
  if (!isBrowser) return;
  try {
    window.localStorage.removeItem(key);
  } catch {
    // ignore
  }
}
