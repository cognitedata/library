import { useMutation } from "@tanstack/react-query";
import { createContext, useContext } from "react";
import {
	aiSelectBestCanvasTemplate,
	aiSelectResource,
	getOrCreateAgent,
} from "../api";
import {
	getAiSelectableTemplates,
	getResourceConfigs,
	isBaseTemplateId,
	useAppConfig,
} from "../app-config";
import { useDefaultWorkPackageStore } from "../contexts/WorkPackageStoreContext";
import type { AiSelections } from "../types";
import { getErrorMessage, toUserFacingErrorMessage } from "../utils/errors";
import { useCdfClientInfo } from "./useClientInfo";

const defaultAiSelectionsDeps = {
	aiSelectBestCanvasTemplate: (
		...args: Parameters<typeof aiSelectBestCanvasTemplate>
	) => aiSelectBestCanvasTemplate(...args),
	aiSelectResource: (...args: Parameters<typeof aiSelectResource>) =>
		aiSelectResource(...args),
	getOrCreateAgent: (...args: Parameters<typeof getOrCreateAgent>) =>
		getOrCreateAgent(...args),
	useWorkPackageStore: useDefaultWorkPackageStore,
	useCdfClientInfo,
};

export type UseAiSelectionsContextType = typeof defaultAiSelectionsDeps;
export const UseAiSelectionsContext = createContext<UseAiSelectionsContextType>(
	defaultAiSelectionsDeps
);

export function useAiCanvasRecommendation() {
	const {
		aiSelectBestCanvasTemplate: selectBestTemplate,
		getOrCreateAgent: getOrCreateAgentFn,
		useWorkPackageStore: useStore,
		useCdfClientInfo: useCdfClient,
	} = useContext(UseAiSelectionsContext);
	const { client, project } = useCdfClient();
	const {
		workOrder,
		asset,
		candidateTemplates,
		candidatesByType,
		setAiSelectedCanvasId,
		setTemplateCanvasId,
		setLoading,
		setError,
	} = useStore();
	const appConfig = useAppConfig();

	return useMutation({
		mutationFn: async () => {
			const aiTemplates = getAiSelectableTemplates(
				candidateTemplates,
				appConfig
			);
			if (!aiTemplates.length) return null;

			if (!client || !project) throw new Error("CDF client not available");
			if (!workOrder || !asset)
				throw new Error("Work order and asset required");

			const agentId = await getOrCreateAgentFn(client, project, appConfig);

			return selectBestTemplate(
				client,
				project,
				agentId,
				aiTemplates,
				workOrder,
				asset,
				candidatesByType
			);
		},
		onMutate: () => {
			setLoading(true, "Atlas AI is analyzing templates...");
			setError(null);
		},
		onSuccess: (selectedCanvasId) => {
			if (selectedCanvasId) {
				setAiSelectedCanvasId(selectedCanvasId);
				setTemplateCanvasId(selectedCanvasId);
			} else {
				setTemplateCanvasId(appConfig.templates.baseTemplate.id);
			}
			setLoading(false);
		},
		onError: (error) => {
			setError(
				`AI canvas recommendation failed: ${toUserFacingErrorMessage(getErrorMessage(error))}`
			);
			setLoading(false);
		},
	});
}

export function useAiSelection() {
	const {
		aiSelectResource: selectResource,
		getOrCreateAgent: getOrCreateAgentFn,
		useWorkPackageStore: useStore,
		useCdfClientInfo: useCdfClient,
	} = useContext(UseAiSelectionsContext);
	const { client, project } = useCdfClient();
	const {
		workOrder,
		asset,
		templateCanvasId,
		templateFields,
		candidatesByType,
		setAiSelections,
		setLoading,
		setError,
	} = useStore();
	const appConfig = useAppConfig();

	return useMutation({
		mutationFn: async (onProgress?: (message: string) => void) => {
			if (!client || !project) throw new Error("CDF client not available");
			if (!workOrder || !asset)
				throw new Error("Work order and asset required");
			if (isBaseTemplateId(templateCanvasId, appConfig)) return {};
			if (!templateFields.length)
				throw new Error("No template fields to select");

			const agentId = await getOrCreateAgentFn(client, project, appConfig);
			const selectedResources = new Set<string>();
			const selections: AiSelections = {};
			const resourceConfigs = getResourceConfigs(appConfig);

			const context = { workOrder, asset };

			for (let i = 0; i < templateFields.length; i++) {
				const field = templateFields[i];
				const config = resourceConfigs[field.type];

				if (!config) {
					continue;
				}

				const instances = candidatesByType[field.type] || [];
				const displayName = config.displayName;

				const message = `Selecting ${displayName.toLowerCase()} for "${field.label}" (${i + 1}/${templateFields.length})...`;
				onProgress?.(message);
				setLoading(true, message);

				const selection = await selectResource(
					client,
					project,
					agentId,
					instances,
					field.type,
					field.label,
					context,
					selectedResources
				);

				if (selection) {
					selections[field.label] = selection;
					selectedResources.add(selection.externalId);
				} else {
					selections[field.label] = null;
				}
			}

			return selections;
		},
		onMutate: () => {
			setLoading(true, "Running AI selection...");
			setError(null);
		},
		onSuccess: (selections) => {
			setAiSelections(selections);
			setLoading(false);
		},
		onError: (error) => {
			setError(
				`AI selection failed: ${toUserFacingErrorMessage(getErrorMessage(error))}`
			);
			setLoading(false);
		},
	});
}
