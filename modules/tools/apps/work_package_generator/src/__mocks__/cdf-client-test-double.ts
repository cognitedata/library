import type { CogniteClient } from "@cognite/sdk";

/**
 * Narrows a partial SDK stub to CogniteClient for unit tests.
 * CogniteClient has a large surface; tests only implement the subset each case needs.
 */
export function cdfClientTestDouble(stub: object): CogniteClient {
	return stub as CogniteClient;
}
