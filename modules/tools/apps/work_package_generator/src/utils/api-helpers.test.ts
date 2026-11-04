import { describe, expect, it, vi } from "vitest";
import { cdfClientTestDouble } from "@/__mocks__/cdf-client-test-double";
import {
	CDF_AI_AGENTS_API_VERSION,
	cdfApiFetch,
	createCdfApiHeaders,
	parseJsonResponse,
} from "./api-helpers";

describe("createCdfApiHeaders", () => {
	it("merges bearer token and defaults with additional headers", async () => {
		const client = cdfClientTestDouble({
			authenticate: vi.fn().mockResolvedValue("tok"),
		});
		const h = await createCdfApiHeaders(client, { "X-Extra": "1" });
		expect(h.Authorization).toBe("Bearer tok");
		expect(h["Content-Type"]).toBe("application/json");
		expect(h["cdf-version"]).toBe(CDF_AI_AGENTS_API_VERSION);
		expect(h["X-Extra"]).toBe("1");
	});
});

describe("cdfApiFetch", () => {
	it("calls fetch with merged headers from object", async () => {
		const client = cdfClientTestDouble({
			authenticate: vi.fn().mockResolvedValue("t"),
		});
		const fetchSpy = vi
			.spyOn(globalThis, "fetch")
			.mockResolvedValue(new Response("{}", { status: 200 }));
		await cdfApiFetch(client, "https://example.com/api", {
			method: "POST",
			headers: { "X-Custom": "y" },
		});
		expect(fetchSpy).toHaveBeenCalledWith(
			"https://example.com/api",
			expect.objectContaining({
				method: "POST",
				headers: expect.objectContaining({
					Authorization: "Bearer t",
					"X-Custom": "y",
				}),
			})
		);
		fetchSpy.mockRestore();
	});

	it("calls fetch with merged headers from Headers object", async () => {
		const client = cdfClientTestDouble({
			authenticate: vi.fn().mockResolvedValue("t"),
		});
		const fetchSpy = vi
			.spyOn(globalThis, "fetch")
			.mockResolvedValue(new Response("{}", { status: 200 }));

		const headersObj = new Headers();
		headersObj.set("X-Custom", "from-headers");

		await cdfApiFetch(client, "https://example.com/api", {
			method: "POST",
			headers: headersObj,
		});
		expect(fetchSpy).toHaveBeenCalledWith(
			"https://example.com/api",
			expect.objectContaining({
				method: "POST",
				headers: expect.objectContaining({
					Authorization: "Bearer t",
					"X-Custom": "from-headers",
				}),
			})
		);
		fetchSpy.mockRestore();
	});

	it("calls fetch with merged headers from array", async () => {
		const client = cdfClientTestDouble({
			authenticate: vi.fn().mockResolvedValue("t"),
		});
		const fetchSpy = vi
			.spyOn(globalThis, "fetch")
			.mockResolvedValue(new Response("{}", { status: 200 }));

		await cdfApiFetch(client, "https://example.com/api", {
			method: "POST",
			headers: [["X-Custom", "from-array"]],
		});
		expect(fetchSpy).toHaveBeenCalledWith(
			"https://example.com/api",
			expect.objectContaining({
				method: "POST",
				headers: expect.objectContaining({
					Authorization: "Bearer t",
					"X-Custom": "from-array",
				}),
			})
		);
		fetchSpy.mockRestore();
	});

	it("works without headers", async () => {
		const client = cdfClientTestDouble({
			authenticate: vi.fn().mockResolvedValue("t"),
		});
		const fetchSpy = vi
			.spyOn(globalThis, "fetch")
			.mockResolvedValue(new Response("{}", { status: 200 }));

		await cdfApiFetch(client, "https://example.com/api", {
			method: "GET",
		});
		expect(fetchSpy).toHaveBeenCalledWith(
			"https://example.com/api",
			expect.objectContaining({
				method: "GET",
				headers: expect.objectContaining({
					Authorization: "Bearer t",
					"Content-Type": "application/json",
				}),
			})
		);
		fetchSpy.mockRestore();
	});
});

describe("parseJsonResponse", () => {
	it("parses ok response", async () => {
		const res = new Response(JSON.stringify({ a: 1 }), { status: 200 });
		await expect(parseJsonResponse(res)).resolves.toEqual({ a: 1 });
	});

	it("throws on non-ok", async () => {
		const res = new Response("", { status: 500, statusText: "Err" });
		await expect(parseJsonResponse(res)).rejects.toThrow(
			"API request failed: Err (500)"
		);
	});
});
