import type { CogniteClient, FileInfo } from '@cognite/sdk';
import {
  getComputedMimeType,
  isNativelySupportedMimeType,
  doesDocumentPreviewApiSupportFile,
  DocumentMimeType,
} from './mimeTypes';

// ============================================================================
// Cache
// ============================================================================

/** CDF URLs expire after 60 min with extendedExpiration — refresh at 59 min. */
const URL_CACHE_EXPIRE_MS = 59 * 60 * 1000;
const MAX_CACHE_SIZE = 200;

interface CacheEntry {
  url: string;
  mimeType: string;
  expiresAt: number;
}

const urlCache = new Map<string, CacheEntry>();

/** Evict expired entries; if still over limit, drop oldest inserted. */
function evictStaleEntries(): void {
  const now = Date.now();
  for (const [key, entry] of urlCache) {
    if (entry.expiresAt <= now) urlCache.delete(key);
  }
  if (urlCache.size > MAX_CACHE_SIZE) {
    for (const key of Array.from(urlCache.keys()).slice(0, urlCache.size - MAX_CACHE_SIZE)) {
      urlCache.delete(key);
    }
  }
}

export function clearFileCache(fileId: number, project?: string): void {
  const prefix = project ? `${project}:` : '';
  urlCache.delete(`${prefix}${fileId}`);
}

export function clearAllFileCache(): void {
  urlCache.clear();
}

// ============================================================================
// Download URL helpers
// ============================================================================

/**
 * Get download URL with extended expiration (59 min instead of default ~30 min).
 * The JS SDK doesn't expose `extendedExpiration`, so we call the API directly.
 */
async function getDownloadUrlExtended(
  client: CogniteClient,
  fileId: number,
): Promise<string> {
  const result = await client.post<{ items: Array<{ downloadUrl: string }> }>(
    `/api/v1/projects/${client.project}/files/downloadlink`,
    {
      data: { items: [{ id: fileId }] },
      params: { extendedExpiration: true },
    },
  );
  const downloadUrl = result.data.items[0]?.downloadUrl;
  if (!downloadUrl) throw new Error(`No download URL for file ${fileId}`);
  return downloadUrl;
}

/**
 * Get a temporary PDF link via the Document Preview API.
 * Converts Office documents to PDF.
 */
async function getPdfTemporaryLink(
  client: CogniteClient,
  fileId: number,
): Promise<string> {
  const response = await client.documents.preview.pdfTemporaryLink(fileId);
  return response.temporaryLink;
}

// ============================================================================
// Main resolution function
// ============================================================================

export interface ResolvedFileConfig {
  url: string;
  mimeType: string;
}

/**
 * Resolve a CDF file to a download URL and effective MIME type.
 *
 * Strategy:
 * 1. Natively supported (images, PDF, text) → direct download with extended expiry
 * 2. Office documents → PDF conversion via Document Preview API
 * 3. Otherwise → throws
 *
 * Results are cached for 59 minutes.
 */
export async function resolveFileDownloadConfig(
  client: CogniteClient,
  file: FileInfo,
): Promise<ResolvedFileConfig> {
  const cacheKey = `${client.project}:${file.id}`;
  const now = Date.now();
  const cached = urlCache.get(cacheKey);
  if (cached && cached.expiresAt > now) {
    return { url: cached.url, mimeType: cached.mimeType };
  }

  const computedMimeType = getComputedMimeType(file);

  let resolved: ResolvedFileConfig;

  if (computedMimeType && isNativelySupportedMimeType(computedMimeType)) {
    const url = await getDownloadUrlExtended(client, file.id);
    resolved = { url, mimeType: computedMimeType };
  } else if (doesDocumentPreviewApiSupportFile(file)) {
    const url = await getPdfTemporaryLink(client, file.id);
    resolved = { url, mimeType: DocumentMimeType.PDF };
  } else {
    throw new Error(
      `Unsupported file type (id: ${file.id}, name: ${file.name}, mimeType: ${file.mimeType})`,
    );
  }

  urlCache.set(cacheKey, { ...resolved, expiresAt: now + URL_CACHE_EXPIRE_MS });
  evictStaleEntries();
  return resolved;
}
