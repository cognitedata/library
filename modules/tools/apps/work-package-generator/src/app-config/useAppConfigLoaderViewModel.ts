import { useCogniteSdk } from "@cognite/app-sdk/react";
import { useQuery } from "@tanstack/react-query";
import { createContext, useCallback, useContext, useMemo } from "react";
import {
	AppConfigLoadError,
	type AppConfigService,
	createDmsAppConfigService,
} from "./AppConfigService";
import { installAppConfig } from "./load";
import type { AppConfig } from "./types";

const defaultDeps = {
	useAppConfigService: (): AppConfigService => {
		const sdk = useCogniteSdk();
		return useMemo(() => createDmsAppConfigService(sdk), [sdk]);
	},
};

export type AppConfigLoaderContextType = typeof defaultDeps;

export const AppConfigLoaderContext =
	createContext<AppConfigLoaderContextType>(defaultDeps);

export interface AppConfigLoaderViewModel {
	isLoading: boolean;
	config: AppConfig | undefined;
	error: AppConfigLoadError | null;
	retry: () => void;
}

export function useAppConfigLoaderViewModel(): AppConfigLoaderViewModel {
	const { useAppConfigService } = useContext(AppConfigLoaderContext);
	const service = useAppConfigService();
	const query = useQuery({
		queryKey: ["wpg-app-config"],
		queryFn: async () => {
			const config = await service.load();
			installAppConfig(config);
			return config;
		},
		retry: false,
	});

	const retry = useCallback(() => {
		void query.refetch();
	}, [query]);

	const isLoading =
		query.isPending || (query.isFetching && query.data === undefined);
	const error = isLoading ? null : toLoadError(query.error);

	return {
		isLoading,
		config: query.data,
		error,
		retry,
	};
}

function toLoadError(error: unknown): AppConfigLoadError | null {
	if (!error) return null;
	if (error instanceof AppConfigLoadError) return error;
	const detail = error instanceof Error ? error.message : "Unknown error";
	return new AppConfigLoadError("network", detail);
}
