import { useMutation } from "@tanstack/react-query";
import { createContext, useContext } from "react";
import { useAppConfig } from "../app-config";
import { getErrorMessage, toUserFacingErrorMessage } from "../utils/errors";
import { defaultCanvasCreationDeps } from "./useCanvasCreation.deps";

export type UseCanvasCreationContextType = typeof defaultCanvasCreationDeps;
export const UseCanvasCreationContext =
	createContext<UseCanvasCreationContextType>(defaultCanvasCreationDeps);

export function useCanvasCreation() {
	const {
		consumeCanvasCleanupIncomplete: consumeCleanupIncomplete,
		createCanvasFromTemplateAndSelections: createCanvas,
		updateCanvasFromTemplateAndSelections: updateCanvas,
		generateCanvasLink: generateLink,
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
				validation: {
					success: true,
					canvasExists: true,
					itemsCount: 0,
					expectedItemsCount: -1,
					itemsMatchExpected: true,
					canvasId,
					errors: [],
				},
				isUpdate: !!existingCanvasId,
				cleanupIncomplete: existingCanvasId
					? consumeCleanupIncomplete(canvasId)
					: false,
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
