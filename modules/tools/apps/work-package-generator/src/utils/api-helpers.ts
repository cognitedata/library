/**
 * API Helper Utilities
 * Utilities for making authenticated API calls to CDF
 */

import type { CogniteClient } from "@cognite/sdk";

/**
 * CDF AI Agents alpha HTTP API version (used by `cdfApiFetch` in `src/api/ai-agent.ts`).
 * Replace with stable `@cognite/sdk` calls and remove this header once the Agents API is GA.
 * Tracking: `reviews/dune-app-review/follow-up-ai-agents-sdk.md`.
 */
export const CDF_AI_AGENTS_API_VERSION = "20250919-alpha" as const;

/**
 * Create headers for CDF API requests
 */
export async function createCdfApiHeaders(
	client: CogniteClient,
	additionalHeaders: Record<string, string> = {}
): Promise<Record<string, string>> {
	const token = await client.authenticate();
	return {
		Authorization: `Bearer ${token}`,
		"Content-Type": "application/json",
		"cdf-version": CDF_AI_AGENTS_API_VERSION,
		...additionalHeaders,
	};
}

/**
 * Convert HeadersInit to Record<string, string> for safe merging
 */
function normalizeHeaders(headers?: HeadersInit): Record<string, string> {
	if (!headers) {
		return {};
	}

	if (headers instanceof Headers) {
		const result: Record<string, string> = {};
		headers.forEach((value, key) => {
			result[key] = value;
		});
		return result;
	}

	if (Array.isArray(headers)) {
		const result: Record<string, string> = {};
		headers.forEach(([key, value]) => {
			result[key] = value;
		});
		return result;
	}

	return headers as Record<string, string>;
}

/**
 * Make an authenticated fetch request to CDF API
 */
export async function cdfApiFetch(
	client: CogniteClient,
	url: string,
	options: RequestInit = {}
): Promise<Response> {
	const { headers: extraHeaders, ...rest } = options;
	const normalizedHeaders = normalizeHeaders(extraHeaders);
	const headers = await createCdfApiHeaders(client, normalizedHeaders);
	return fetch(url, {
		...rest,
		headers,
	});
}

/**
 * Parse JSON response with error handling
 */
export async function parseJsonResponse<T>(response: Response): Promise<T> {
	if (!response.ok) {
		throw new Error(
			`API request failed: ${response.statusText} (${response.status})`
		);
	}
	return response.json() as Promise<T>;
}
