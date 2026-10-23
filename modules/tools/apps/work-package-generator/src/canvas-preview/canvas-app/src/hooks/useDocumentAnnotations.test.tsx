import type { CogniteClient } from "@cognite/sdk";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";

import { SDKProvider } from "../sdk/SDKProvider";
import { useDocumentAnnotations } from "./useDocumentAnnotations";

const viewKey = "CogniteDiagramAnnotation/v1";

function edge(overrides: {
	externalId: string;
	page?: number;
	status?: string;
	type?: string;
	properties?: Record<string, unknown> | null;
}) {
	const properties =
		overrides.properties === null
			? {}
			: {
					cdf_cdm: {
						[viewKey]: {
							status: overrides.status ?? "Approved",
							startNodePageNumber: overrides.page ?? 1,
							startNodeXMin: 0.1,
							startNodeXMax: 0.2,
							startNodeYMin: 0.3,
							startNodeYMax: 0.4,
							startNodeText: "P-100",
						},
					},
				};
	return {
		instanceType: "edge" as const,
		externalId: overrides.externalId,
		space: "ann",
		type: { externalId: overrides.type ?? "diagrams.AssetLink" },
		endNode: { space: "assets", externalId: "a1" },
		properties,
	};
}

function wrapper(query: ReturnType<typeof vi.fn>) {
	const client = { instances: { query } } as object as CogniteClient;
	const queryClient = new QueryClient({
		defaultOptions: { queries: { retry: false } },
	});
	return function Wrapper({ children }: { children: ReactNode }) {
		return (
			<QueryClientProvider client={queryClient}>
				<SDKProvider client={client} schedule={(fn) => fn()}>
					{children}
				</SDKProvider>
			</QueryClientProvider>
		);
	};
}

const fileSource = {
	type: "fdm" as const,
	instance: {
		space: "files",
		externalId: "pid",
		viewExternalId: "CogniteFile",
		viewSpace: "cdf_cdm",
		viewVersion: "v1",
	},
};

describe("useDocumentAnnotations", () => {
	it("keeps approved annotations on the requested page and pages the query", async () => {
		const page = Array.from({ length: 10000 }, (_, index) =>
			edge({ externalId: `e${index}` })
		);
		const query = vi
			.fn()
			.mockResolvedValueOnce({
				items: { annotations: page },
				nextCursor: { annotations: "next" },
			})
			.mockResolvedValueOnce({
				items: {
					annotations: [
						edge({ externalId: "kept", type: "diagrams.FileLink" }),
						edge({ externalId: "other-page", page: 2 }),
						edge({ externalId: "rejected", status: "Rejected" }),
						edge({ externalId: "no-props", properties: null }),
						{ instanceType: "node", externalId: "skip" },
					],
				},
			});

		const { result } = renderHook(() => useDocumentAnnotations(fileSource, 1), {
			wrapper: wrapper(query),
		});

		await waitFor(() => expect(result.current.isLoading).toBe(false));
		expect(query).toHaveBeenCalledTimes(2);
		expect(result.current.annotations.map((item) => item.id)).toEqual(
			expect.arrayContaining([
				expect.stringContaining("kept"),
				expect.stringContaining("e0"),
			])
		);
		expect(
			result.current.annotations.some((item) => item.id.includes("rejected"))
		).toBe(false);
		expect(
			result.current.annotations.find((item) => item.id.includes("kept"))
				?.resourceType
		).toBe("file");
	});

	it("returns no annotations without an fdm file", async () => {
		const query = vi.fn();
		const { result } = renderHook(() => useDocumentAnnotations(undefined), {
			wrapper: wrapper(query),
		});
		await waitFor(() => expect(result.current.isLoading).toBe(false));
		expect(result.current.annotations).toEqual([]);
		expect(query).not.toHaveBeenCalled();
	});
});
