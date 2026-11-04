import { useQueries, useQuery } from "@tanstack/react-query";
import { createContext, useContext, useEffect, useMemo } from "react";
import {
	findCanvasesByName,
	getTemplateCanvasInstances,
	listTemplateCanvases,
} from "../api";
import {
	dataModelView,
	getBaseTemplateCanvas,
	getResourceConfigs,
	isBaseTemplateId,
	useAppConfig,
} from "../app-config";
import { useDefaultWorkPackageStore } from "../contexts/WorkPackageStoreContext";
import type { TemplateCanvas, TemplateInstance } from "../types";
import { useCdfClientInfo } from "./useClientInfo";

const defaultTemplatesDeps = {
	findCanvasesByName: (...args: Parameters<typeof findCanvasesByName>) =>
		findCanvasesByName(...args),
	getTemplateCanvasInstances: (
		...args: Parameters<typeof getTemplateCanvasInstances>
	) => getTemplateCanvasInstances(...args),
	listTemplateCanvases: (...args: Parameters<typeof listTemplateCanvases>) =>
		listTemplateCanvases(...args),
	useWorkPackageStore: useDefaultWorkPackageStore,
	useCdfClientInfo,
};

export type UseTemplatesContextType = typeof defaultTemplatesDeps;
export const UseTemplatesContext =
	createContext<UseTemplatesContextType>(defaultTemplatesDeps);

export function useTemplateCanvases() {
	const {
		listTemplateCanvases: listTemplates,
		useWorkPackageStore: useStore,
		useCdfClientInfo: useCdfClient,
	} = useContext(UseTemplatesContext);
	const { client } = useCdfClient();
	const {
		setCandidateTemplates,
		setTemplateCanvasId,
		candidateTemplates,
		templateCanvasId,
	} = useStore();

	const appConfig = useAppConfig();
	const baseTemplate = useMemo(
		() => getBaseTemplateCanvas(appConfig),
		[appConfig]
	);

	const query = useQuery({
		queryKey: ["templateCanvases"],
		queryFn: async () => {
			if (!client) throw new Error("CDF client not available");
			return listTemplates(client, appConfig);
		},
		enabled: !!client,
		staleTime: 5 * 60 * 1000,
	});

	useEffect(() => {
		if (query.data) {
			const withBase = [baseTemplate, ...query.data];
			setCandidateTemplates(withBase);
			if (!templateCanvasId) {
				setTemplateCanvasId(appConfig.templates.baseTemplate.id);
			}
		}
	}, [
		query.data,
		setCandidateTemplates,
		setTemplateCanvasId,
		templateCanvasId,
		appConfig,
		baseTemplate,
	]);

	return {
		templates:
			candidateTemplates.length > 0 ? candidateTemplates : query.data || [],
		isLoading: query.isLoading,
		error: query.error,
		refetch: query.refetch,
	};
}

export function useFindCanvasesByName(canvasName: string) {
	const { findCanvasesByName: findByName, useCdfClientInfo: useCdfClient } =
		useContext(UseTemplatesContext);
	const { client } = useCdfClient();

	return useQuery({
		queryKey: ["canvasesByName", canvasName],
		queryFn: async () => {
			if (!client || !canvasName.trim()) return [];
			return findByName(client, canvasName);
		},
		enabled: !!client && !!canvasName.trim(),
		staleTime: 30 * 1000,
	});
}

// ---------------------------------------------------------------------------
// Template slots (pre-loads canvas contents for display)
// ---------------------------------------------------------------------------

export interface TemplateSlot {
	label: string;
	viewExternalId: string;
	category:
		| "asset"
		| "workorder"
		| "activity"
		| "equipment"
		| "file"
		| "timeseries"
		| "other";
	populatedBy: "auto" | "ai";
}

function categorizeInstance(
	inst: TemplateInstance,
	config?: Parameters<typeof getResourceConfigs>[0]
): TemplateSlot | null {
	const viewExt = inst.viewExternalId || "";
	const label = inst.label || "";
	const labelLower = label.toLowerCase();

	if (
		viewExt === dataModelView("asset", config).externalId &&
		labelLower.startsWith("main")
	) {
		return {
			label,
			viewExternalId: viewExt,
			category: "asset",
			populatedBy: "auto",
		};
	}
	if (
		viewExt === dataModelView("activity", config).externalId &&
		labelLower.startsWith("main")
	) {
		return {
			label,
			viewExternalId: viewExt,
			category: "workorder",
			populatedBy: "auto",
		};
	}

	const resourceConfigs = getResourceConfigs(config);
	const matchingType = Object.entries(resourceConfigs).find(
		([, conf]) => conf.viewId.externalId === viewExt
	);
	if (matchingType) {
		const [typeKey] = matchingType;
		return {
			label,
			viewExternalId: viewExt,
			category: typeKey as TemplateSlot["category"],
			populatedBy: "ai",
		};
	}

	if (viewExt) {
		return {
			label,
			viewExternalId: viewExt,
			category: "other",
			populatedBy: "auto",
		};
	}

	return null;
}

export function useAllTemplateSlots(templates: TemplateCanvas[]) {
	const {
		getTemplateCanvasInstances: getTemplateInstances,
		useCdfClientInfo: useCdfClient,
	} = useContext(UseTemplatesContext);
	const { client } = useCdfClient();
	const appConfig = useAppConfig();

	// Exclude the synthetic base template — it has no canvas instances to fetch.
	// Filtering (vs enabled: false) avoids a permanently pending query observer.
	const fetchableTemplates = useMemo(
		() => templates.filter((t) => !isBaseTemplateId(t.externalId, appConfig)),
		[templates, appConfig]
	);

	const queries = useQueries({
		queries: fetchableTemplates.map((t) => ({
			queryKey: ["templateSlots", t.externalId],
			queryFn: async () => {
				if (!client) throw new Error("No client");
				const instances = await getTemplateInstances(
					client,
					t.externalId,
					appConfig
				);
				return instances
					.map((instance) => categorizeInstance(instance, appConfig))
					.filter((s): s is TemplateSlot => s !== null);
			},
			enabled: !!client,
			staleTime: 5 * 60 * 1000,
		})),
	});

	const slotsByTemplate = useMemo(() => {
		const map: Record<string, TemplateSlot[]> = {};
		fetchableTemplates.forEach((t, i) => {
			const query = queries[i];
			if (query?.data) {
				map[t.externalId] = query.data;
			}
		});
		return map;
	}, [fetchableTemplates, queries]);

	const isLoading = queries.some((q) => q.isLoading);
	const isFetchingSlots = queries.some((q) => q.isFetching);
	const slotsError = queries.some((q) => q.isError);
	const firstSlotsError = queries.find((q) => q.isError)?.error ?? null;

	const refetchSlots = () => Promise.all(queries.map((q) => q.refetch()));

	return {
		slotsByTemplate,
		isLoading,
		isFetchingSlots,
		slotsError,
		firstSlotsError,
		refetchSlots,
	};
}
