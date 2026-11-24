/**
 * File Resolution Service
 *
 * Provides file URL resolution with:
 * - Extended expiration (59 minutes instead of default ~30 minutes)
 * - Document Preview API fallback for Office documents
 * - In-memory caching with auto-expiration
 */

import type { CogniteClient } from "@cognite/sdk";
import {
	DocumentMimeType,
	doesDocumentPreviewApiSupportFile,
	getComputedMimeTypeFromFileInfo,
	isNativelySupportedMimeType,
} from "@dune-canvas/canvas-lib";
import { type CdfSchedule, defaultCdfSchedule } from "../../../cdfSchedule";

// ============================================================================
// Configuration
// ============================================================================

/**
 * Default cache expiration time (59 minutes).
 * CDF file URLs expire after 60 minutes when extendedExpiration is set to true.
 * We use 59 minutes to ensure we refresh before expiration.
 */
export const DEFAULT_URL_CACHE_EXPIRE_MS = 59 * 60 * 1000;

// ============================================================================
// Types
// ============================================================================

export interface ResolvedFileConfig {
	url: string;
	mimeType: string;
}

export interface FileInfo {
	id: number;
	mimeType?: string;
	name: string;
}

export interface ResolveFileOptions {
	/**
	 * Cache expiration time in milliseconds.
	 * Defaults to DEFAULT_URL_CACHE_EXPIRE_MS (59 minutes).
	 */
	cacheExpireMs?: number;
	/** Defaults to the app CDF task runner. */
	schedule?: CdfSchedule;
}

// ============================================================================
// Cache
// ============================================================================

interface CacheEntry {
	config: ResolvedFileConfig;
	expiresAt: number;
}

const urlCache = new Map<string, CacheEntry>();

/**
 * Get a cached value or resolve it.
 * If the cached value has expired, it will be refreshed.
 */
async function getCachedOrResolve(
	key: string,
	resolver: () => Promise<ResolvedFileConfig>,
	expireMs: number
): Promise<ResolvedFileConfig> {
	const now = Date.now();
	const cached = urlCache.get(key);

	if (cached && cached.expiresAt > now) {
		return cached.config;
	}

	const config = await resolver();
	urlCache.set(key, {
		config,
		expiresAt: now + expireMs,
	});

	return config;
}

// ============================================================================
// Extended Expiration Workaround
// ============================================================================

/**
 * Get download URL with extended expiration (59 minutes instead of default).
 *
 * The JS SDK doesn't support the `extendedExpiration` query parameter,
 * so we make a direct POST call to the API.
 *
 * @see https://cognitedata.atlassian.net/browse/AH-2564
 */
async function getDownloadUrlWithExtendedExpiration(
	client: CogniteClient,
	fileId: number,
	schedule: CdfSchedule
): Promise<string> {
	const result = await schedule(() =>
		client.post<{
			items: Array<{ downloadUrl: string }>;
		}>(`/api/v1/projects/${client.project}/files/downloadlink`, {
			data: { items: [{ id: fileId }] },
			params: { extendedExpiration: true },
		})
	);

	const downloadUrl = result.data.items[0]?.downloadUrl;
	if (!downloadUrl) {
		throw new Error(`Failed to get download URL for file ${fileId}`);
	}

	return downloadUrl;
}

// ============================================================================
// Document Preview API
// ============================================================================

/**
 * Get a temporary PDF link for a file via the Document Preview API.
 * This converts Office documents (Word, Excel, PowerPoint) to PDF.
 */
async function getPdfTemporaryLink(
	client: CogniteClient,
	fileId: number,
	schedule: CdfSchedule
): Promise<string> {
	const response = await schedule(() =>
		client.documents.preview.pdfTemporaryLink(fileId)
	);
	return response.temporaryLink;
}

// ============================================================================
// Main Resolution Function
// ============================================================================

/**
 * Resolve a file to a download URL and MIME type.
 *
 * Resolution strategy:
 * 1. If the file's MIME type is natively supported (images, PDF, video, text),
 *    get a download URL with extended expiration (59 minutes).
 * 2. If the Document Preview API supports the file (Word, Excel, PowerPoint),
 *    get a PDF conversion via pdfTemporaryLink.
 * 3. Otherwise, throw an error for unsupported file types.
 *
 * Results are cached with auto-expiration (default 59 minutes).
 *
 * @example
 * const config = await resolveFileDownloadConfig(client, {
 *   id: 12345,
 *   mimeType: 'image/png',
 *   name: 'photo.png'
 * });
 * // config = { url: 'https://...', mimeType: 'image/png' }
 *
 * @example
 * const config = await resolveFileDownloadConfig(client, {
 *   id: 12345,
 *   mimeType: 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
 *   name: 'document.docx'
 * });
 * // config = { url: 'https://... (PDF preview)', mimeType: 'application/pdf' }
 */
export async function resolveFileDownloadConfig(
	client: CogniteClient,
	file: FileInfo,
	options?: ResolveFileOptions
): Promise<ResolvedFileConfig> {
	const cacheExpireMs = options?.cacheExpireMs ?? DEFAULT_URL_CACHE_EXPIRE_MS;
	const schedule = options?.schedule ?? defaultCdfSchedule;
	const cacheKey = String(file.id);

	return getCachedOrResolve(
		cacheKey,
		async () => {
			const computedMimeType = getComputedMimeTypeFromFileInfo(file);

			// Strategy 1: Natively supported MIME types - get direct download URL
			if (computedMimeType && isNativelySupportedMimeType(computedMimeType)) {
				const url = await getDownloadUrlWithExtendedExpiration(
					client,
					file.id,
					schedule
				);
				return {
					url,
					mimeType: computedMimeType,
				};
			}

			// Strategy 2: Document Preview API can convert to PDF
			if (doesDocumentPreviewApiSupportFile(file)) {
				const url = await getPdfTemporaryLink(client, file.id, schedule);
				return {
					url,
					mimeType: DocumentMimeType.PDF,
				};
			}

			// Strategy 3: Unsupported file type
			throw new Error(
				`Unsupported file type (id: ${file.id}, name: ${file.name}, mimeType: ${file.mimeType})`
			);
		},
		cacheExpireMs
	);
}
