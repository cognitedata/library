/**
 * UUID Generation Utilities
 * Utilities for generating unique identifiers
 */

/**
 * Generate a cryptographically secure UUID v4 identifier.
 *
 * Uses the Web Crypto API (`crypto.randomUUID()`), which is available in all
 * modern browsers and Node.js ≥ 14.17.
 *
 * @returns A UUID string in the format xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx
 *
 * @example
 * ```ts
 * const id = generateUuid(); // 'a1b2c3d4-e5f6-4789-a012-b3c4d5e6f789'
 * ```
 */
export function generateUuid(): string {
	return crypto.randomUUID();
}
