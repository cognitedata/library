import { useMutation } from "@tanstack/react-query";
import { createContext, useContext } from "react";
import {
	createCanvasFromTemplateAndSelections,
	generateCanvasLink,
	updateCanvasFromTemplateAndSelections,
	validateCanvasCreation,
} from "../api";
import { buildBaseTemplateItems } from "../api/canvas-builder/base-template";
import {
	getTemplateCanvasAnnotations,
	getTemplateCanvasInstances,
} from "../api/retrieve-data";
import {
	fusionOrganizationFromRuntime,
	getAppConfig,
	isBaseTemplateId,
	useAppConfig,
} from "../app-config";
import { useWorkPackageStore } from "../store";
import { getErrorMessage, toUserFacingErrorMessage } from "../utils/errors";
import { useCdfClientInfo } from "./useClientInfo";

const defaultCanvasCreationDeps = {
	createCanvasFromTemplateAndSelections,
	updateCanvasFromTemplateAndSelections,
	validateCanvasCreation,
	generateCanvasLink,
	buildBaseTemplateItems,
	getTemplateCanvasInstances,
	getTemplateCanvasAnnotations,
	useWorkPackageStore,
	useCdfClientInfo,
	getOrg: () => {
		const config = getAppConfig();
		const organization =
			fusionOrganizationFromRuntime(
				{
					hostname: window.location.hostname,
					ancestorOrigins: readAncestorOrigins(),
					referrer: document.referrer,
				},
				config.fusion.hostSuffix
			) ?? config.fusion.fallbackOrganization;
		if (!organization) {
			throw new Error(
				"Fusion organization is not available. Open the app from Fusion, or set fusion.fallbackOrganization on the config node."
			);
		}
		return organization;
	},
};

export type UseCanvasCreationContextType = typeof defaultCanvasCreationDeps;
export const UseCanvasCreationContext =
	createContext<UseCanvasCreationContextType>(defaultCanvasCreationDeps);

function readAncestorOrigins(): string[] {
	const location = window.location as Location & {
		ancestorOrigins?: ArrayLike<string>;
	};
	const origins = location.ancestorOrigins;
	if (!origins) return [];
	return Array.from(origins);
}

export function useCanvasCreation() {
	const {
		createCanvasFromTemplateAndSelections: createCanvas,
		updateCanvasFromTemplateAndSelections: updateCanvas,
		validateCanvasCreation: validateCanvas,
		generateCanvasLink: generateLink,
		buildBaseTemplateItems: buildBaseItems,
		getTemplateCanvasInstances: getTemplateInstances,
		getTemplateCanvasAnnotations: getTemplateAnnotations,
		useWorkPackageStore: useStore,
		useCdfClientInfo: useCdfClient,
		getOrg,
	} = useContext(UseCanvasCreationContext);
	const { client, project, cluster } = useCdfClient();
	const appConfig = useAppConfig();
	const {
		workOrder,
		asset,
		templateCanvasId,
		aiSelections,
		candidatesByType,
		config,
		setError,
		setLoading,
	} = useStore();

	return useMutation({
		mutationFn: async ({
			canvasName,
			existingCanvasId,
		}: {
			canvasName: string;
			existingCanvasId?: string;
		}) => {
			if (!client) throw new Error("CDF client not available");
			if (!workOrder || !asset)
				throw new Error("Work order and asset required");
			if (!templateCanvasId) throw new Error("No template canvas selected");

			let canvasId: string;

			if (existingCanvasId) {
				canvasId = await updateCanvas(
					client,
					config,
					existingCanvasId,
					canvasName,
					templateCanvasId,
					aiSelections,
					asset,
					workOrder,
					candidatesByType,
					appConfig
				);
			} else {
				canvasId = await createCanvas(
					client,
					config,
					canvasName,
					templateCanvasId,
					aiSelections,
					asset,
					workOrder,
					candidatesByType,
					appConfig
				);
			}

			let expectedCount: number;
			if (isBaseTemplateId(templateCanvasId, appConfig)) {
				expectedCount = buildBaseItems(
					config,
					workOrder,
					asset,
					candidatesByType,
					appConfig
				).length;
			} else {
				const templateInstances = await getTemplateInstances(
					client,
					templateCanvasId,
					appConfig
				);
				const templateAnnotations = await getTemplateAnnotations(
					client,
					templateCanvasId,
					appConfig
				);
				expectedCount = templateInstances.length + templateAnnotations.length;
			}

			const validation = await validateCanvas(
				client,
				config,
				canvasId,
				expectedCount,
				appConfig
			);

			const canvasLink = generateLink(
				canvasId,
				project,
				cluster,
				getOrg(),
				appConfig
			);

			return {
				canvasId,
				canvasLink,
				validation,
				isUpdate: !!existingCanvasId,
			};
		},
		onMutate: () => {
			setLoading(true, "Creating canvas...");
			setError(null);
		},
		onSuccess: () => {
			setLoading(false);
		},
		onError: (error) => {
			const detail = toUserFacingErrorMessage(getErrorMessage(error));
			setError(`Canvas creation failed: ${detail}`);
			setLoading(false);
		},
	});
}
