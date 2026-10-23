import type { CogniteClient } from "@cognite/sdk";
import { describe, expect, it, vi } from "vitest";

import { resolveFileDownloadConfig } from "./fileResolution";

function client(overrides: object = {}): CogniteClient {
	return {
		project: "demo",
		post: vi.fn(async () => ({
			status: 200,
			headers: {},
			data: { items: [{ downloadUrl: "https://files.example.test/a" }] },
		})),
		documents: {
			preview: {
				pdfTemporaryLink: vi.fn(async () => ({
					temporaryLink: "https://files.example.test/preview.pdf",
				})),
			},
		},
		...overrides,
	} as object as CogniteClient;
}

describe("resolveFileDownloadConfig", () => {
	it("downloads a natively supported file", async () => {
		const sdk = client();
		const config = await resolveFileDownloadConfig(sdk, {
			id: 11,
			mimeType: "image/png",
			name: "a.png",
		});
		expect(config).toEqual({
			url: "https://files.example.test/a",
			mimeType: "image/png",
		});
		await resolveFileDownloadConfig(sdk, {
			id: 11,
			mimeType: "image/png",
			name: "a.png",
		});
		expect(sdk.post).toHaveBeenCalledTimes(1);
	});

	it("schedules the download request through the CDF runner", async () => {
		const calls: Array<() => Promise<unknown>> = [];
		const schedule = <T>(fn: () => Promise<T>): Promise<T> => {
			calls.push(fn);
			return fn();
		};
		await resolveFileDownloadConfig(
			client(),
			{ id: 99, mimeType: "image/png", name: "b.png" },
			{ schedule }
		);
		expect(calls).toHaveLength(1);
	});

	it("converts an office file through the preview API", async () => {
		const config = await resolveFileDownloadConfig(client(), {
			id: 22,
			mimeType:
				"application/vnd.openxmlformats-officedocument.wordprocessingml.document",
			name: "a.docx",
		});
		expect(config.mimeType).toBe("application/pdf");
	});

	it("refetches after the cache window expires", async () => {
		vi.useFakeTimers();
		const sdk = client();
		const file = { id: 55, mimeType: "image/png", name: "c.png" };
		await resolveFileDownloadConfig(sdk, file, { cacheExpireMs: 1_000 });
		vi.setSystemTime(Date.now() + 1_001);
		await resolveFileDownloadConfig(sdk, file, { cacheExpireMs: 1_000 });
		expect(sdk.post).toHaveBeenCalledTimes(2);
		vi.useRealTimers();
	});

	it("throws for an unsupported type and a missing download url", async () => {
		await expect(
			resolveFileDownloadConfig(client(), {
				id: 33,
				mimeType: "application/zip",
				name: "a.zip",
			})
		).rejects.toThrow(/Unsupported file type/);

		const sdk = client({
			post: vi.fn(async () => ({
				status: 200,
				headers: {},
				data: { items: [{}] },
			})),
		});
		await expect(
			resolveFileDownloadConfig(sdk, {
				id: 44,
				mimeType: "image/png",
				name: "b.png",
			})
		).rejects.toThrow(/Failed to get download URL/);
	});
});
