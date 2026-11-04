import {
	createContext,
	useCallback,
	useContext,
	useEffect,
	useMemo,
	useState,
} from "react";
import { getCanvasCreationSubtitle } from "@/components/work-package/canvas-creation/get-canvas-creation-subtitle";
import type { CanvasResult } from "@/components/work-package/canvas-creation/types";
import { useCanvasDraftPreview } from "@/hooks/useCanvasDraftPreview";
import type { AiSelections, TemplateField } from "@/types";
import { getWorkflowRecoveryHints } from "@/utils";

type CanvasDraftPreviewHook = (
	isReady: boolean
) => Pick<
	ReturnType<typeof useCanvasDraftPreview>,
	"data" | "isPending" | "isFetching" | "isPlaceholderData" | "error"
>;

const defaultCanvasCreationSectionDeps: {
	useCanvasDraftPreview: CanvasDraftPreviewHook;
} = {
	useCanvasDraftPreview,
};

export type CanvasCreationSectionViewModelContextType =
	typeof defaultCanvasCreationSectionDeps;
export const CanvasCreationSectionViewModelContext =
	createContext<CanvasCreationSectionViewModelContextType>(
		defaultCanvasCreationSectionDeps
	);

export type CanvasCreationSectionViewModelInput = {
	isReady: boolean;
	hasExistingCanvas: boolean;
	canvasResult?: CanvasResult;
	templateFields: TemplateField[];
	aiSelections: AiSelections;
	onCreateCanvas: () => void;
};

export function useCanvasCreationSectionViewModel({
	isReady,
	hasExistingCanvas,
	canvasResult,
	templateFields,
	aiSelections,
	onCreateCanvas,
}: CanvasCreationSectionViewModelInput) {
	const { useCanvasDraftPreview: useDraftPreview } = useContext(
		CanvasCreationSectionViewModelContext
	);
	const draftPreview = useDraftPreview(isReady);
	const [existingSaveDialogOpen, setExistingSaveDialogOpen] = useState(false);
	const isSavedSuccessfully = !!canvasResult?.validation.success;

	const recoveryHints = useMemo(() => {
		if (!canvasResult) return [];
		return getWorkflowRecoveryHints({
			templateFields,
			aiSelections,
			validation: canvasResult.validation,
		});
	}, [canvasResult, templateFields, aiSelections]);

	useEffect(() => {
		if (!hasExistingCanvas) setExistingSaveDialogOpen(false);
	}, [hasExistingCanvas]);

	const handleSaveClick = useCallback(() => {
		if (hasExistingCanvas) {
			setExistingSaveDialogOpen(true);
			return;
		}
		onCreateCanvas();
	}, [hasExistingCanvas, onCreateCanvas]);

	return {
		draftPreview,
		existingSaveDialogOpen,
		setExistingSaveDialogOpen,
		isSavedSuccessfully,
		recoveryHints,
		subtitle: getCanvasCreationSubtitle(isReady, canvasResult),
		handleSaveClick,
	};
}
