import { apiUrl } from "./apiBase";

export async function fetchJson<T>(path: string, init?: RequestInit): Promise<T> {
  const r = await fetch(apiUrl(path), init);
  if (!r.ok) {
    const body = (await r.json().catch(() => ({}))) as { detail?: string };
    throw new Error(String(body.detail ?? r.status));
  }
  return r.json() as Promise<T>;
}
