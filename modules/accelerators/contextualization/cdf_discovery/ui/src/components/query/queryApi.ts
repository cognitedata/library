export async function readFetchError(r: Response): Promise<string> {
  let msg = r.statusText;
  try {
    const j = (await r.json()) as { detail?: unknown };
    const d = j?.detail;
    if (typeof d === "string") {
      msg = d;
    } else if (Array.isArray(d)) {
      msg = d
        .map((x) => (x && typeof x === "object" && "msg" in x ? String((x as { msg?: unknown }).msg) : ""))
        .filter(Boolean)
        .join("; ");
    }
  } catch {
    /* ignore */
  }
  return msg;
}

export async function fetchJson<T>(path: string): Promise<T> {
  const r = await fetch(path);
  if (!r.ok) {
    throw new Error(await readFetchError(r));
  }
  return r.json() as Promise<T>;
}

export async function postPreviewJson<T>(path: string, body: unknown): Promise<T> {
  const r = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) {
    throw new Error(await readFetchError(r));
  }
  return r.json() as Promise<T>;
}
