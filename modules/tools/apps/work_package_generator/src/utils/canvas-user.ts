/**
 * Resolve the CDF user identity stored on industrial canvas root nodes.
 */

import type { CogniteClient } from "@cognite/sdk";
import { type AppConfig, resolveAppConfig } from "../app-config";

let cachedCanvasUserId: Promise<string> | undefined;

/**
 * Returns the current principal id for canvas metadata (`createdBy` / `updatedBy`).
 * Prefers `userIdentifier` from `profiles.me()` (stable CDF principal id).
 * Display name is not used: Fusion shows "Unknown" when it cannot resolve a profile.
 * Falls back to `config.canvas.userId` when the profile API is unavailable.
 */
export async function resolveCanvasUserId(
	client: CogniteClient,
	config?: AppConfig
): Promise<string> {
	cachedCanvasUserId ??= fetchCanvasUserId(client, config);
	return cachedCanvasUserId;
}

/** Test-only: clear the in-memory profile cache. */
export function resetCanvasUserIdCache(): void {
	cachedCanvasUserId = undefined;
}

async function fetchCanvasUserId(
	client: CogniteClient,
	config?: AppConfig
): Promise<string> {
	try {
		const profile = await client.profiles.me();

		const userIdentifier =
			typeof profile.userIdentifier === "string"
				? profile.userIdentifier.trim()
				: "";
		if (userIdentifier) return userIdentifier;

		const email = typeof profile.email === "string" ? profile.email.trim() : "";
		if (email) return email;
	} catch {
		// Use default when running without profile scope or offline
	}

	return resolveAppConfig(config).canvas.userId;
}
