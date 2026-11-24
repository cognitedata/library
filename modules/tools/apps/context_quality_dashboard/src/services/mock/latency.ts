export type LatencyMode = number | 'random';

export async function withLatency<T>(operation: () => T, latency: LatencyMode): Promise<T> {
  const delayMs =
    latency === 'random' ? 200 + Math.floor(Math.random() * 301) : latency;
  await new Promise<void>((resolve) => {
    setTimeout(resolve, delayMs);
  });
  return operation();
}
