import { useQuery } from "@tanstack/react-query";
import { createContext, useContext, useEffect } from "react";
import {
	getResourceConfigs,
	isBaseTemplateId,
	useAppConfig,
} from "../app-config";
import type { TemplateField } from "../types";
import { isMainAsset, isMainWorkOrder } from "../utils/canvas-item-helpers";
import { defaultTemplateFieldsDeps } from "./useTemplateFields.deps";

export type UseTemplateFieldsContextType = typeof defaultTemplateFieldsDeps;
export const UseTemplateFieldsContext =
	createContext<UseTemplateFieldsContextType>(defaultTemplateFieldsDeps);

export function useTemplateFields(options?: { queryEnabled?: boolean }) {
	const {
		getTemplateCanvasInstances: getTemplateInstances,
		useWorkPackageStore: useStore,
		useCdfClientInfo: useCdfClient,
	} = useContext(UseTemplateFieldsContext);
	const queryEnabled = options?.queryEnabled ?? true;
	const { client } = useCdfClient();
	const {
		templateCanvasId,
		templateFieldsLoadedFrom,
		setTemplateFields,
		setTemplateFieldsLoadedFrom,
		templateFields,
	} = useStore();

	const appConfig = useAppConfig();
	const shouldFetch = !!(
		queryEnabled &&
		client &&
		templateCanvasId &&
		!isBaseTemplateId(templateCanvasId, appConfig) &&
		templateFieldsLoadedFrom !== templateCanvasId
	);

	const query = useQuery({
		queryKey: ["templateFields", templateCanvasId],
		queryFn: async () => {
			if (!client || !templateCanvasId) return [];

			const instances = await getTemplateInstances(
				client,
				templateCanvasId,
				appConfig
			);
			const resourceConfigs = getResourceConfigs(appConfig);
			const fields: TemplateField[] = [];

			for (const inst of instances) {
				const viewExt = inst.viewExternalId;
				const label = inst.label || "";

				if (!viewExt) continue;
				if (isMainAsset(inst, appConfig) || isMainWorkOrder(inst, appConfig)) {
					continue;
				}

				const matchingConfig = Object.entries(resourceConfigs).find(
					([, conf]) => conf.viewId.externalId === viewExt
				);

				if (matchingConfig) {
					const [typeKey] = matchingConfig;
					fields.push({
						id: inst.id?.trim() || `${viewExt}:${label}:${fields.length}`,
						type: typeKey,
						label,
						viewExternalId: viewExt,
						viewSpace: inst.viewSpace,
						viewVersion: inst.viewVersion,
					});
				}
			}

			return fields;
		},
		enabled: shouldFetch,
		staleTime: 5 * 60 * 1000,
	});

	useEffect(() => {
		if (query.data && templateCanvasId && queryEnabled) {
			setTemplateFields(query.data);
			setTemplateFieldsLoadedFrom(templateCanvasId);
		}
	}, [
		query.data,
		templateCanvasId,
		queryEnabled,
		setTemplateFields,
		setTemplateFieldsLoadedFrom,
	]);

	const fields = !queryEnabled
		? []
		: templateFields.length > 0
			? templateFields
			: query.data || [];

	return {
		fields,
		isLoading: queryEnabled && query.isLoading,
		error: query.error,
		refetch: query.refetch,
	};
}
