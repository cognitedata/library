import { backoffDelayMs, fetchWithBackoff } from './fetchWithBackoff';

function answer(status: number, headers: Record<string, string> = {}): Response {
  return new Response('{}', { status, headers });
}

describe('backoffDelayMs', () => {
  it('should honour Retry-After over the computed backoff', () => {
    expect(backoffDelayMs(3, '2')).toBe(2000);
  });

  it('should spread the exponential backoff between half and all of its ceiling', () => {
    expect(backoffDelayMs(2, null, () => 0)).toBe(300);
    expect(backoffDelayMs(2, null, () => 1)).toBe(600);
  });
});

describe('fetchWithBackoff', () => {
  it('should send the request again after a 429 and return the next answer', async () => {
    const fetchFn = vi.fn().mockResolvedValueOnce(answer(429, { 'Retry-After': '1' })).mockResolvedValueOnce(answer(200));
    const sleep = vi.fn().mockResolvedValue(undefined);

    const response = await fetchWithBackoff(fetchFn, 'https://x', {}, sleep);

    expect(response.status).toBe(200);
    expect(fetchFn).toHaveBeenCalledTimes(2);
    expect(sleep).toHaveBeenCalledWith(1000);
  });

  it('should give back the 429 once the attempts are used up', async () => {
    const fetchFn = vi.fn().mockImplementation(() => Promise.resolve(answer(429)));

    const response = await fetchWithBackoff(fetchFn, 'https://x', {}, () => Promise.resolve());

    expect(response.status).toBe(429);
    expect(fetchFn).toHaveBeenCalledTimes(5);
  });

  it('should not retry other failures', async () => {
    const fetchFn = vi.fn().mockResolvedValue(answer(500));

    await fetchWithBackoff(fetchFn, 'https://x', {}, () => Promise.resolve());

    expect(fetchFn).toHaveBeenCalledTimes(1);
  });
});
