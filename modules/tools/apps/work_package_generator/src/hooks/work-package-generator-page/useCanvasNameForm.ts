/**
 * Canvas name state, debounce, existing-canvas lookup, and create/update handlers.
 */

import {
	createContext,
	useCallback,
	useContext,
	useEffect,
	useState,
} from "react";
import type { WorkOrder } from "@/types";
import { getErrorMessage, toUserFacingErrorMessage } from "@/utils/errors";
import { useFindCanvasesByName } from "../useTemplates";

const defaultCanvasNameFormDeps = {
	useFindCanvasesByName,
};

export type UseCanvasNameFormContextType = typeof defaultCanvasNameFormDeps;
export const UseCanvasNameFormContext =
	createContext<UseCanvasNameFormContextType>(defaultCanvasNameFormDeps);

type CreateCanvasArgs = {
	canvasName: string;
	existingCanvasId?: string;
};

export type CanvasNameFormInput = {
	workOrder: WorkOrder | null;
	createCanvas: (args: CreateCanvasArgs) => void;
};

export function useCanvasNameForm({
	workOrder,
	createCanvas,
}: CanvasNameFormInput) {
	const { useFindCanvasesByName: useFindByName } = useContext(
		UseCanvasNameFormContext
	);
	const defaultCanvasName = workOrder
		? `Canvas for ${workOrder.name || workOrder.externalId}`
		: "";

	const [canvasName, setCanvasName] = useState(defaultCanvasName);
	const [debouncedCanvasName, setDebouncedCanvasName] =
		useState(defaultCanvasName);

	useEffect(() => {
		if (workOrder) {
			const name = `Canvas for ${workOrder.name || workOrder.externalId}`;
			setCanvasName(name);
			setDebouncedCanvasName(name);
		}
	}, [workOrder]);

	useEffect(() => {
		const timer = setTimeout(() => setDebouncedCanvasName(canvasName), 500);
		return () => clearTimeout(timer);
	}, [canvasName]);

	const {
		data: existingCanvases,
		isLoading: isCheckingExists,
		error: existingCanvasLookupError,
		refetch: refetchExistingCanvases,
	} = useFindByName(debouncedCanvasName);

	const handleCreateCanvas = useCallback(
		() => createCanvas({ canvasName }),
		[createCanvas, canvasName]
	);

	const handleUpdateCanvas = useCallback(() => {
		if (existingCanvases?.[0]) {
			createCanvas({
				canvasName,
				existingCanvasId: existingCanvases[0].externalId,
			});
		}
	}, [createCanvas, canvasName, existingCanvases]);

	const resetCanvasNameForm = useCallback(() => {
		setCanvasName("");
		setDebouncedCanvasName("");
	}, []);

	const handleRetryExistingCanvasLookup = useCallback(() => {
		void refetchExistingCanvases();
	}, [refetchExistingCanvases]);

	const hasExistingCanvas = (existingCanvases?.length || 0) > 0;
	const existingCanvasLookupErrorMessage = existingCanvasLookupError
		? toUserFacingErrorMessage(getErrorMessage(existingCanvasLookupError))
		: null;

	return {
		canvasName,
		setCanvasName,
		isCheckingExists,
		hasExistingCanvas,
		existingCanvasLookupErrorMessage,
		onRetryExistingCanvasLookup: handleRetryExistingCanvasLookup,
		onCreateCanvas: handleCreateCanvas,
		onUpdateCanvas: handleUpdateCanvas,
		resetCanvasNameForm,
	};
}
