/**
 * SDK Provider
 * Provides the CogniteClient to the data-resolution hooks via React Context
 */

import type { CogniteClient } from "@cognite/sdk";
import type React from "react";
import { createContext, useContext, useMemo } from "react";

import { type CdfSchedule, defaultCdfSchedule } from "../../../cdfSchedule";

interface SDKContextValue {
	client: CogniteClient;
	schedule: CdfSchedule;
}

const SDKContext = createContext<SDKContextValue | null>(null);

export interface SDKProviderProps {
	client: CogniteClient;
	/** Defaults to the app CDF task runner (concurrency + 429 retry). */
	schedule?: CdfSchedule;
	children: React.ReactNode;
}

export const SDKProvider: React.FC<SDKProviderProps> = ({
	client,
	schedule = defaultCdfSchedule,
	children,
}) => {
	const value = useMemo(() => ({ client, schedule }), [client, schedule]);

	return <SDKContext.Provider value={value}>{children}</SDKContext.Provider>;
};

/**
 * Hook to optionally access the CogniteClient from any component
 * Returns null if used outside of SDKProvider (useful for components that work with or without CDF)
 */
export function useCogniteClientOptional(): CogniteClient | null {
	const context = useContext(SDKContext);
	return context?.client ?? null;
}

/** CDF scheduler from SDKProvider, or the app default when no provider is mounted. */
export function useCdfSchedule(): CdfSchedule {
	const context = useContext(SDKContext);
	return context?.schedule ?? defaultCdfSchedule;
}
