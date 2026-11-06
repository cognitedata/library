const MAX_ATTEMPTS = 5;
const BACKOFF_BASE_MS = 300;

export type Sleep = (ms: number) => Promise<void>;

export function defaultSleep(ms: number): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
}

/** How long to wait before the next try: the server's Retry-After, else exponential backoff with jitter. */
export function backoffDelayMs(attempt: number, retryAfter: string | null, random: () => number = Math.random): number {
  const seconds = Number(retryAfter);
  if (retryAfter !== null && Number.isFinite(seconds) && seconds > 0) {
    return seconds * 1000;
  }
  const ceiling = BACKOFF_BASE_MS * 2 ** (attempt - 1);
  return ceiling / 2 + random() * (ceiling / 2);
}

/**
 * `fetchFn`, tried again while the service answers 429 (too many requests), at most `MAX_ATTEMPTS` times.
 * A 429 means the request was not processed, so a POST can safely be sent again. Any other answer is returned.
 */
export async function fetchWithBackoff(
  fetchFn: typeof fetch,
  url: string,
  init: RequestInit,
  sleep: Sleep = defaultSleep
): Promise<Response> {
  for (let attempt = 1; ; attempt += 1) {
    const response = await fetchFn(url, init);
    if (response.status !== 429 || attempt >= MAX_ATTEMPTS) {
      return response;
    }
    await sleep(backoffDelayMs(attempt, response.headers.get('Retry-After')));
  }
}
