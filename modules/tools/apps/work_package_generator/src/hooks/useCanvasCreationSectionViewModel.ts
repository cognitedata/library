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
import type { AiSelections, TemplateField, ValidationResult } from "@/types";
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
	isCheckingExists?: boolean;
	recheckCanvasName?: () => Promise<boolean>;
};

export function useCanvasCreationSectionViewModel({
	isReady,
	hasExistingCanvas,
	canvasResult,
	templateFields,
	aiSelections,
	onCreateCanvas,
	isCheckingExists = false,
	recheckCanvasName,
}: CanvasCreationSectionViewModelInput) {
	const { useCanvasDraftPreview: useDraftPreview } = useContext(
		CanvasCreationSectionViewModelContext
	);
	const draftPreview = useDraftPreview(isReady);
	const [existingSaveDialogOpen, setExistingSaveDialogOpen] = useState(false);
	const isSavedSuccessfully = canvasResult != null;
	const savedResult = useMemo(() => {
		if (!canvasResult) return undefined;
		const validation: ValidationResult = {
			success: true,
			canvasExists: true,
			itemsCount: 0,
			expectedItemsCount: -1,
			itemsMatchExpected: true,
			canvasId: canvasResult.canvasId,
			errors: [],
		};
		return { ...canvasResult, validation };
	}, [canvasResult]);

	const recoveryHints = useMemo(() => {
		if (!savedResult) return [];
		return getWorkflowRecoveryHints({
			templateFields,
			aiSelections,
			validation: savedResult.validation,
		});
	}, [savedResult, templateFields, aiSelections]);

	useEffect(() => {
		if (!hasExistingCanvas) setExistingSaveDialogOpen(false);
	}, [hasExistingCanvas]);

	const handleSaveClick = useCallback(() => {
		if (isCheckingExists) return;
		void (async () => {
			if (hasExistingCanvas) {
				setExistingSaveDialogOpen(true);
				return;
			}
			if (recheckCanvasName) {
				try {
					const exists = await recheckCanvasName();
					if (exists) {
						setExistingSaveDialogOpen(true);
						return;
					}
				} catch {
					return;
				}
			}
			onCreateCanvas();
		})();
	}, [hasExistingCanvas, isCheckingExists, onCreateCanvas, recheckCanvasName]);

	return {
		draftPreview,
		existingSaveDialogOpen,
		setExistingSaveDialogOpen,
		isSavedSuccessfully,
		recoveryHints,
		subtitle: getCanvasCreationSubtitle(isReady, savedResult),
		handleSaveClick,
	};
}
