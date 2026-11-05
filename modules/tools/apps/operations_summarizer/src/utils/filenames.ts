/**
 * Filename generation for CDF and downloads.
 */

const ISO_PUNCTUATION = /[:.]/g;

function toDateString(d: Date | string): string {
  return typeof d === "string" ? d : d.toISOString().split("T")[0];
}

/**
 * Generate a filename for CDF (safe, no extension) or download (with extension).
 * @param prefix - Filename prefix (e.g. "ops", "operation_summary")
 * @param startDate - Start date (Date or YYYY-MM-DD string)
 * @param endDate - End date (Date or YYYY-MM-DD string)
 * @param extension - Optional extension (e.g. "md", "pdf"); when set, returns download-style name with ".{extension}"
 */
export function generateFilename(
  prefix: string,
  startDate: Date | string,
  endDate: Date | string,
  extension?: string,
): string {
  const start = toDateString(startDate);
  const end = toDateString(endDate);

  if (extension != null && extension !== "") {
    const timestamp = new Date()
      .toISOString()
      .replace(ISO_PUNCTUATION, "")
      .slice(0, 15);
    return `${prefix}_${start}_to_${end}_${timestamp}.${extension}`;
  }

  const now = new Date().toISOString().replace(ISO_PUNCTUATION, "-");
  const filename = `${prefix}_${start}_${end}_${now}`
    .replace(/[:\s()]/g, "_")
    .replace(/[^a-zA-Z0-9_.-]/g, "");
  return filename.length > 100 ? filename.slice(0, 100) : filename;
}
