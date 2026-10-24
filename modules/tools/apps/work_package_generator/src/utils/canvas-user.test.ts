import { describe, expect, it, vi } from "vitest";
import { DEFAULT_USER_ID } from "@/__mocks__/app-config";
import { cdfClientTestDouble } from "../__mocks__/cdf-client-test-double";
import { resetCanvasUserIdCache, resolveCanvasUserId } from "./canvas-user";

describe(resolveCanvasUserId.name, () => {
	it("prefers userIdentifier over displayName", async () => {
		resetCanvasUserIdCache();
		const me = vi.fn().mockResolvedValue({
			userIdentifier: "principal-123",
			displayName: "Unknown",
			email: "user@example.test",
		});
		const client = cdfClientTestDouble({ profiles: { me } });

		await expect(resolveCanvasUserId(client)).resolves.toBe("principal-123");
	});

	it("falls back to email when userIdentifier is missing", async () => {
		resetCanvasUserIdCache();
		const me = vi.fn().mockResolvedValue({
			displayName: "Unknown",
			email: "user@example.test",
		});
		const client = cdfClientTestDouble({ profiles: { me } });

		await expect(resolveCanvasUserId(client)).resolves.toBe(
			"user@example.test"
		);
	});

	it("falls back to default when profiles.me fails", async () => {
		resetCanvasUserIdCache();
		const me = vi.fn().mockRejectedValue(new Error("no profile"));
		const client = cdfClientTestDouble({ profiles: { me } });

		await expect(resolveCanvasUserId(client)).resolves.toBe(DEFAULT_USER_ID);
	});
});
