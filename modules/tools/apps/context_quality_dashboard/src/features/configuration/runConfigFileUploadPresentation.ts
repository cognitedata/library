/** Human-readable size for a config file shown in the upload card. */
export function formatRunConfigFileSize(byteLength: number): string {
  if (byteLength < 1024) {
    return `${String(byteLength)} B`;
  }
  if (byteLength < 1024 * 1024) {
    const kilobytes = byteLength / 1024;
    const rounded = Math.round(kilobytes * 10) / 10;
    const text = rounded % 1 === 0 ? String(Math.round(rounded)) : rounded.toFixed(1);
    return `${text} KB`;
  }
  return `${(byteLength / (1024 * 1024)).toFixed(1)} MB`;
}

export function runConfigFileByteLength(text: string): number {
  return new TextEncoder().encode(text).length;
}
