import type { CogniteClient } from "@cognite/sdk";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";

import { SDKProvider } from "../sdk/SDKProvider";
import {
	useResolvedFile,
	useResolvedTableData,
	useResolvedTimeseries,
} from "./useResolvedData";

function wrapperFor(client: CogniteClient) {
	const queryClient = new QueryClient({
		defaultOptions: { queries: { retry: false } },
	});
	return function Wrapper({ children }: { children: ReactNode }) {
		return (
			<QueryClientProvider client={queryClient}>
				<SDKProvider client={client}>{children}</SDKProvider>
			</QueryClientProvider>
		);
	};
}

function clientWith(datapoints: unknown[]): CogniteClient {
	return {
		project: "demo",
		post: vi.fn(async () => ({
			data: { items: [{ downloadUrl: "https://files.example.test/a" }] },
		})),
		files: {
			retrieve: vi.fn(async () => [
				{ id: 7, mimeType: "image/png", name: "a.png" },
			]),
		},
		instances: {
			retrieve: vi.fn(async () => ({
				items: [
					{
						space: "s",
						externalId: "asset",
						properties: {
							cdf_cdm: {
								"CogniteAsset/v1": { name: "Pump", pressure: 3 },
							},
						},
					},
				],
			})),
			list: vi.fn(async () => ({ items: [] })),
			upsert: vi.fn(async () => ({ items: [] })),
			delete: vi.fn(async () => undefined),
			aggregate: vi.fn(async () => ({ items: [] })),
			search: vi.fn(async () => ({ items: [] })),
			query: vi.fn(async () => ({})),
		},
		datapoints: {
			retrieve: vi.fn(async () => datapoints),
			insert: vi.fn(),
			retrieveLatest: vi.fn(async () => []),
			delete: vi.fn(),
		},
		timeseries: {
			retrieve: vi.fn(),
			list: vi.fn(),
			update: vi.fn(),
			search: vi.fn(),
		},
		documents: { preview: { pdfTemporaryLink: vi.fn() } },
	} as object as CogniteClient;
}

describe("useResolvedTimeseries", () => {
	it("returns direct series without a client", async () => {
		const { result } = renderHook(
			() =>
				useResolvedTimeseries({
					type: "direct",
					series: [{ x: 1, y: 2 }],
				}),
			{ wrapper: wrapperFor(clientWith([])) }
		);
		await waitFor(() => expect(result.current.isLoading).toBe(false));
		expect(result.current.series).toEqual([{ x: 1, y: 2 }]);
	});

	it("returns raw points when the aggregate count is small", async () => {
		const client = clientWith([
			{
				datapoints: [{ timestamp: 1, average: 2, min: 1, max: 3, count: 1 }],
			},
		]);
		vi.mocked(client.datapoints.retrieve)
			.mockResolvedValueOnce([
				{
					datapoints: [{ timestamp: 1, average: 2, min: 1, max: 3, count: 1 }],
				},
			] as never)
			.mockResolvedValueOnce([
				{ datapoints: [{ timestamp: 1, value: 4 }] },
			] as never);
		const { result } = renderHook(
			() =>
				useResolvedTimeseries({
					type: "fdm",
					instance: {
						space: "s",
						externalId: "ts",
						viewExternalId: "CogniteTimeSeries",
					},
					startDate: "2026-01-01T00:00:00.000Z",
					endDate: "2026-01-02T00:00:00.000Z",
				}),
			{ wrapper: wrapperFor(client) }
		);
		await waitFor(() => expect(result.current.isLoading).toBe(false));
		expect(result.current.series[0]?.y).toBe(4);
	});

	it("requests aggregates then raw points by instance id and epoch-ms range", async () => {
		// Arrange
		const client = clientWith([]);
		vi.mocked(client.datapoints.retrieve)
			.mockResolvedValueOnce([
				{ datapoints: [{ timestamp: 1, average: 2, count: 1 }] },
			] as never)
			.mockResolvedValueOnce([{ datapoints: [] }] as never);
		const start = Date.parse("2026-01-01T00:00:00.000Z");
		const end = Date.parse("2026-01-02T00:00:00.000Z");

		// Act
		const { result } = renderHook(
			() =>
				useResolvedTimeseries({
					type: "fdm",
					instance: {
						space: "s",
						externalId: "ts",
						viewExternalId: "CogniteTimeSeries",
					},
					startDate: "2026-01-01T00:00:00.000Z",
					endDate: "2026-01-02T00:00:00.000Z",
				}),
			{ wrapper: wrapperFor(client) }
		);
		await waitFor(() => expect(result.current.isLoading).toBe(false));

		// Assert
		const calls = vi.mocked(client.datapoints.retrieve).mock.calls;
		expect(calls[0]?.[0]).toEqual({
			items: [
				{
					instanceId: { space: "s", externalId: "ts" },
					aggregates: ["average", "min", "max", "count"],
					granularity: expect.any(String),
				},
			],
			start,
			end,
			limit: 2000,
		});
		expect(calls[1]?.[0]).toEqual({
			items: [{ instanceId: { space: "s", externalId: "ts" } }],
			start,
			end,
			limit: 500,
		});
	});

	it("returns no points for an fdm source outside the CogniteTimeSeries view", async () => {
		const client = clientWith([]);
		const { result } = renderHook(
			() =>
				useResolvedTimeseries({
					type: "fdm",
					instance: {
						space: "s",
						externalId: "ts",
						viewExternalId: "PumpSensor",
					},
				}),
			{ wrapper: wrapperFor(client) }
		);
		await waitFor(() => expect(result.current.isLoading).toBe(false));
		expect(result.current.series).toEqual([]);
		expect(result.current.error).toBeNull();
	});

	it("keeps aggregated buckets when the count is large", async () => {
		const client = clientWith([
			{
				datapoints: [
					{ timestamp: 1, average: 2, min: 1, max: 9, count: 100000 },
				],
			},
		]);
		const { result } = renderHook(
			() =>
				useResolvedTimeseries({
					type: "fdm",
					instance: {
						space: "s",
						externalId: "ts",
						viewExternalId: "CogniteTimeSeries",
					},
				}),
			{ wrapper: wrapperFor(client) }
		);
		await waitFor(() =>
			expect(result.current.series.length).toBeGreaterThan(0)
		);
		expect(result.current.series[0]).toMatchObject({ y: 2, count: 100000 });
	});
});

describe("useResolvedFile", () => {
	it("returns a direct url", async () => {
		const { result } = renderHook(
			() =>
				useResolvedFile({
					type: "direct",
					url: "https://files.example.test/a.png",
				}),
			{
				wrapper: ({ children }) => (
					<QueryClientProvider client={new QueryClient()}>
						{children}
					</QueryClientProvider>
				),
			}
		);
		await waitFor(() => expect(result.current.isLoading).toBe(false));
		expect(result.current.url).toContain("a.png");
	});

	it("resolves an fdm file through the download api", async () => {
		const { result } = renderHook(
			() =>
				useResolvedFile({
					type: "fdm",
					instance: {
						space: "s",
						externalId: "file",
						viewExternalId: "CogniteFile",
					},
				}),
			{ wrapper: wrapperFor(clientWith([])) }
		);
		await waitFor(() => expect(result.current.isLoading).toBe(false));
		expect(result.current.url).toContain("files.example.test");
	});

	it("returns no url for an fdm source outside the CogniteFile view", async () => {
		const client = clientWith([]);
		const { result } = renderHook(
			() =>
				useResolvedFile({
					type: "fdm",
					instance: {
						space: "s",
						externalId: "file",
						viewExternalId: "PumpManual",
					},
				}),
			{ wrapper: wrapperFor(client) }
		);
		await waitFor(() => expect(result.current.isLoading).toBe(false));
		expect(result.current.url).toBe("");
		expect(result.current.error).toBeNull();
		expect(client.files.retrieve).not.toHaveBeenCalled();
	});
});

describe("useResolvedTimeseries ranges", () => {
	it("uses the caller time range and reports a fetch failure", async () => {
		const client = clientWith([]);
		vi.mocked(client.datapoints.retrieve).mockRejectedValue(
			new Error("unavailable")
		);
		const { result } = renderHook(
			() =>
				useResolvedTimeseries(
					{
						type: "fdm",
						instance: {
							space: "s",
							externalId: "ts",
							viewExternalId: "CogniteTimeSeries",
						},
						startDate: "2020-01-01T00:00:00.000Z",
					},
					{
						timeRange: {
							start: new Date("2020-01-01T00:00:00.000Z"),
							end: new Date("2024-01-01T00:00:00.000Z"),
						},
					}
				),
			{ wrapper: wrapperFor(client) }
		);
		await waitFor(() => expect(result.current.error).toBeTruthy());
		expect(result.current.series).toEqual([]);
	});

	it("returns no points when the aggregate response is empty", async () => {
		const client = clientWith([]);
		vi.mocked(client.datapoints.retrieve).mockResolvedValue([
			{ datapoints: [] },
		] as never);
		const { result } = renderHook(
			() =>
				useResolvedTimeseries({
					type: "fdm",
					instance: {
						space: "s",
						externalId: "ts",
						viewExternalId: "CogniteTimeSeries",
					},
					endDate: "2024-06-01T00:00:00.000Z",
				}),
			{ wrapper: wrapperFor(client) }
		);
		await waitFor(() => expect(result.current.isLoading).toBe(false));
		expect(result.current.series).toEqual([]);
	});

	it("maps raw points and treats a missing value as zero", async () => {
		const client = clientWith([]);
		vi.mocked(client.datapoints.retrieve)
			.mockResolvedValueOnce([
				{ datapoints: [{ timestamp: new Date(5), count: 1 }] },
			] as never)
			.mockResolvedValueOnce([
				{ datapoints: [{ timestamp: new Date(5), value: undefined }] },
			] as never);
		const { result } = renderHook(
			() =>
				useResolvedTimeseries(
					{
						type: "fdm",
						instance: {
							space: "s",
							externalId: "ts",
							viewExternalId: "CogniteTimeSeries",
						},
					},
					{ enabled: true }
				),
			{ wrapper: wrapperFor(client) }
		);
		await waitFor(() =>
			expect(result.current.series).toEqual([{ x: 5, y: 0 }])
		);
	});
});

describe("useResolvedFile sources", () => {
	it("resolves a classic file id and a missing file", async () => {
		const client = clientWith([]);
		const { result } = renderHook(
			() => useResolvedFile({ type: "cdf", fileId: 7 }),
			{ wrapper: wrapperFor(client) }
		);
		await waitFor(() =>
			expect(result.current.url).toContain("files.example.test")
		);

		vi.mocked(client.files.retrieve).mockResolvedValue([]);
		const missing = renderHook(
			() => useResolvedFile({ type: "cdf", fileId: 99 }),
			{ wrapper: wrapperFor(client) }
		);
		await waitFor(() => expect(missing.result.current.error).toBeTruthy());
	});

	it("stays idle when disabled and when there is no source", async () => {
		const { result } = renderHook(() => useResolvedFile(undefined), {
			wrapper: wrapperFor(clientWith([])),
		});
		await waitFor(() => expect(result.current.isLoading).toBe(false));
		expect(result.current.url).toBeNull();

		const disabled = renderHook(
			() =>
				useResolvedTimeseries(
					{
						type: "direct",
						series: [{ x: 1, y: 2 }],
					},
					{ enabled: false }
				),
			{ wrapper: wrapperFor(clientWith([])) }
		);
		expect(disabled.result.current.series).toEqual([]);
	});
});

describe("useResolvedTableData", () => {
	it("returns direct rows", async () => {
		const { result } = renderHook(
			() =>
				useResolvedTableData({
					type: "direct",
					columns: [{ key: "name", label: "Name" }],
					rows: [{ name: "Pump" }],
				}),
			{
				wrapper: ({ children }) => (
					<QueryClientProvider client={new QueryClient()}>
						{children}
					</QueryClientProvider>
				),
			}
		);
		await waitFor(() => expect(result.current.isLoading).toBe(false));
		expect(result.current.rows).toEqual([{ name: "Pump" }]);
	});

	it("formats fdm properties into rows", async () => {
		const client = clientWith([]);
		vi.mocked(client.instances.retrieve).mockResolvedValue({
			items: [
				{
					space: "s",
					externalId: "asset",
					properties: {
						cdf_cdm: {
							"CogniteAsset/v1": {
								name: "Pump",
								isActive: true,
								tags: ["a", "b"],
								parent: { space: "s", externalId: "parent" },
								meta: { note: "x" },
								empty: null,
								zIndex: 1,
							},
						},
					},
				},
			],
		} as never);
		const { result } = renderHook(
			() =>
				useResolvedTableData({
					type: "fdm",
					instance: {
						space: "s",
						externalId: "asset",
						viewSpace: "cdf_cdm",
						viewExternalId: "CogniteAsset",
						viewVersion: "v1",
					},
				}),
			{ wrapper: wrapperFor(client) }
		);
		await waitFor(() => expect(result.current.rows.length).toBeGreaterThan(2));
		expect(result.current.rows.map((row) => row.value)).toEqual(
			expect.arrayContaining(["Pump", "Yes", "[2 items]", "s:parent", "—"])
		);
	});

	it("uses the alternate property key and reports a missing instance", async () => {
		const client = clientWith([]);
		vi.mocked(client.instances.retrieve).mockResolvedValue({
			items: [
				{
					properties: {
						s: { CogniteAsset: { name: "Alt" } },
					},
				},
			],
		} as never);
		const { result } = renderHook(
			() =>
				useResolvedTableData({
					type: "fdm",
					instance: {
						space: "s",
						externalId: "asset",
						viewExternalId: "CogniteAsset",
					},
				}),
			{ wrapper: wrapperFor(client) }
		);
		await waitFor(() =>
			expect(result.current.rows.map((row) => row.value)).toContain("Alt")
		);

		vi.mocked(client.instances.retrieve).mockResolvedValue({
			items: [],
		} as never);
		const missing = renderHook(
			() =>
				useResolvedTableData({
					type: "fdm",
					instance: {
						space: "s",
						externalId: "gone",
						viewExternalId: "CogniteAsset",
					},
				}),
			{ wrapper: wrapperFor(client) }
		);
		await waitFor(() => expect(missing.result.current.error).toBeTruthy());
	});
});
