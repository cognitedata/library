import type { CogniteClient } from "@cognite/sdk";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { type ReactNode, useState } from "react";
import { AppConfigProvider } from "@/app-config";
import {
	type CdfRuntimeContextValue,
	CdfRuntimeTestProvider,
} from "@/contexts/CdfRuntimeContext";
import { cdfClientTestDouble } from "./cdf-client-test-double";

export function createStubCdfClient(overrides?: {
	project?: string;
	getBaseUrl?: () => string;
	instances?: unknown;
}): CogniteClient {
	return cdfClientTestDouble({
		project: "test-project",
		getBaseUrl: () => "https://test.cognitedata.com",
		instances: {
			list: async () => ({ items: [] }),
			query: async () => ({ items: { nodes: [], edges: [] }, nextCursor: {} }),
			search: async () => ({ items: [] }),
			upsert: async () => undefined,
			delete: async () => undefined,
		},
		...overrides,
	});
}

export function createCdfTestRuntime(
	overrides?: Partial<CdfRuntimeContextValue>
): CdfRuntimeContextValue {
	const client = overrides?.client ?? createStubCdfClient();
	return {
		client,
		project: overrides?.project ?? "test-project",
		cluster: overrides?.cluster ?? "test",
	};
}

/** Combines CdfRuntimeTestProvider + QueryClientProvider for hook tests. */
export function cdfRuntimeQueryWrapper(
	cdf?: CdfRuntimeContextValue,
	queryClientConfig?: ConstructorParameters<typeof QueryClient>[0]
) {
	const value = cdf ?? createCdfTestRuntime();
	return function Wrapper({ children }: { children: ReactNode }) {
		const [queryClient] = useState(
			() =>
				new QueryClient(
					queryClientConfig ?? {
						defaultOptions: {
							queries: { retry: false },
							mutations: { retry: false },
						},
					}
				)
		);
		return (
			<AppConfigProvider>
				<CdfRuntimeTestProvider value={value}>
					<QueryClientProvider client={queryClient}>
						{children}
					</QueryClientProvider>
				</CdfRuntimeTestProvider>
			</AppConfigProvider>
		);
	};
}
