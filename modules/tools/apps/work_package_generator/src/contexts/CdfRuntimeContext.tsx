import { useCogniteSdk } from "@cognite/app-sdk/react";
import type { CogniteClient } from "@cognite/sdk";
import { createContext, type ReactNode, useContext, useMemo } from "react";
import { extractClusterFromUrl, useAppConfig } from "@/app-config";

export interface CdfRuntimeContextValue {
	client: CogniteClient | undefined;
	project: string;
	cluster: string;
}

const CdfRuntimeContext = createContext<CdfRuntimeContextValue | undefined>(
	undefined
);

export function CdfRuntimeProvider({ children }: { children: ReactNode }) {
	const sdk = useCogniteSdk();
	const appConfig = useAppConfig();

	const project = sdk?.project ?? "";
	const clusterResult = useMemo(() => {
		if (!sdk?.project) {
			return { error: "CDF session is missing a project" };
		}
		try {
			return { cluster: extractClusterFromUrl(sdk.getBaseUrl(), appConfig) };
		} catch (error) {
			const detail = error instanceof Error ? error.message : "unknown error";
			return {
				error: `Could not read the CDF cluster from the session URL: ${detail}`,
			};
		}
	}, [sdk, appConfig]);

	const value = useMemo<CdfRuntimeContextValue | null>(() => {
		const cluster =
			"cluster" in clusterResult ? clusterResult.cluster : undefined;
		if (!sdk || !project || !cluster) return null;
		return { client: sdk, project, cluster };
	}, [sdk, project, clusterResult]);

	if (!value) {
		throw new Error(
			"error" in clusterResult
				? clusterResult.error
				: "CDF session is missing a project"
		);
	}

	return (
		<CdfRuntimeContext.Provider value={value}>
			{children}
		</CdfRuntimeContext.Provider>
	);
}

/** Test-only provider: inject CDF runtime without CogniteSdkProvider. */
export function CdfRuntimeTestProvider({
	value,
	children,
}: {
	value: CdfRuntimeContextValue;
	children: ReactNode;
}) {
	return (
		<CdfRuntimeContext.Provider value={value}>
			{children}
		</CdfRuntimeContext.Provider>
	);
}

export function useCdfRuntime(): CdfRuntimeContextValue {
	const context = useContext(CdfRuntimeContext);
	if (context === undefined) {
		throw new Error("useCdfRuntime must be used within a CdfRuntimeProvider");
	}
	return context;
}
