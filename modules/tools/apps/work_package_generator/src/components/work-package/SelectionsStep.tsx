import { Sparkles } from "lucide-react";
import { useMemo, useState } from "react";
import { isBaseTemplateId, useAppConfig } from "@/app-config";
import { AI_UNAVAILABLE_MESSAGE, useAiRunUi } from "@/hooks/useAiSelections";
import type {
	AiSelections,
	ResourceInstance,
	TemplateField,
	WorkOrder,
} from "@/types";
import { selectionSlotKey } from "@/utils/canvas-item-helpers";
import { PickResourceModal } from "./PickResourceModal";
import { AutomationReviewSection } from "./selections-step/AutomationReviewSection";
import { SelectionProgress } from "./selections-step/SelectionProgress";
import { SelectionSlotCard } from "./selections-step/SelectionSlotCard";
import { SelectionsEmptyState } from "./selections-step/SelectionsEmptyState";
import { getCandidatesForSlotType } from "./slot-helpers";

interface SelectionsStepProps {
	/** When null, empty state matches sidebar “begin here” guidance. */
	workOrder: WorkOrder | null;
	templateCanvasId: string | null;
	templateFields: TemplateField[];
	aiSelections: AiSelections;
	candidatesByType?: Record<string, ResourceInstance[]>;
	isRunningAiSelection: boolean;
	isLoadingFields: boolean;
	templateFieldsErrorMessage?: string | null;
	onRetryTemplateFields?: () => void;
	/** Resolved template name for the review summary. */
	templateDisplayName?: string | null;
	/** Show post-automation summary and gate until user continues to Create. */
	showAutomationReview?: boolean;
	onContinueToCreate?: () => void;
	onGoToTemplate?: () => void;
	onGoToResources?: () => void;
	showAiUnavailable?: boolean;
	onRerunAi?: () => void;
	onRerunSlot?: (slotKey: string) => void;
	slotErrors?: Record<string, string>;
}

export function SelectionsStep({
	workOrder,
	templateCanvasId,
	templateFields,
	aiSelections,
	candidatesByType = {},
	isRunningAiSelection,
	isLoadingFields,
	templateFieldsErrorMessage = null,
	onRetryTemplateFields,
	templateDisplayName = null,
	showAutomationReview = false,
	onContinueToCreate,
	onGoToTemplate,
	onGoToResources,
	showAiUnavailable = false,
	onRerunAi,
	onRerunSlot,
	slotErrors,
}: SelectionsStepProps) {
	const appConfig = useAppConfig();
	const runUi = useAiRunUi();
	const rerunAi = onRerunAi ?? runUi.rerunAll;
	const rerunSlot = onRerunSlot ?? runUi.rerunSlot;
	const errors = slotErrors ?? runUi.slotErrors;
	const aiUnavailable = showAiUnavailable || runUi.selectionFailed;
	const [pickModalSlot, setPickModalSlot] = useState<string | null>(null);
	const isBaseTemplate = isBaseTemplateId(templateCanvasId, appConfig);

	const { filled, total, emptySlots } = useMemo(() => {
		const f = templateFields.filter(
			(tf) => aiSelections[selectionSlotKey(tf)]
		).length;
		const empty = templateFields
			.filter((tf) => !aiSelections[selectionSlotKey(tf)])
			.map((tf) => tf.label);
		return { filled: f, total: templateFields.length, emptySlots: empty };
	}, [templateFields, aiSelections]);

	const pickedField = templateFields.find(
		(field) => selectionSlotKey(field) === pickModalSlot
	);

	return (
		<div className="p-3 space-y-3">
			<h3 className="text-sm font-semibold flex items-center gap-2">
				<Sparkles className="h-4 w-4" />
				AI Resource Selections
			</h3>

			{showAutomationReview && onContinueToCreate && (
				<AutomationReviewSection
					templateDisplayName={templateDisplayName}
					filled={filled}
					total={total}
					emptySlotCount={emptySlots.length}
					onContinueToCreate={onContinueToCreate}
					onGoToTemplate={onGoToTemplate}
					onGoToResources={onGoToResources}
				/>
			)}
			{aiUnavailable && !isRunningAiSelection && (
				<p className="text-xs text-destructive" role="alert">
					{AI_UNAVAILABLE_MESSAGE}
				</p>
			)}
			<SelectionProgress
				filled={filled}
				total={total}
				isRunningAiSelection={isRunningAiSelection}
				onRerunAi={rerunAi ?? undefined}
			/>
			{total === 0 ? (
				<SelectionsEmptyState
					workOrder={workOrder}
					templateCanvasId={templateCanvasId}
					isBaseTemplate={isBaseTemplate}
					isLoadingFields={isLoadingFields}
					templateFieldsErrorMessage={templateFieldsErrorMessage}
					onRetryTemplateFields={onRetryTemplateFields}
				/>
			) : (
				<div className="space-y-2">
					{templateFields.map((field) => (
						<SelectionSlotCard
							key={selectionSlotKey(field)}
							field={field}
							selection={aiSelections[selectionSlotKey(field)] ?? null}
							canPickManually={
								getCandidatesForSlotType(field.type, candidatesByType).length >
								0
							}
							isRunningAiSelection={isRunningAiSelection}
							slotError={errors[selectionSlotKey(field)] || null}
							onRetrySlot={rerunSlot ?? undefined}
							onPickManually={setPickModalSlot}
						/>
					))}
				</div>
			)}

			{pickModalSlot && (
				<PickResourceModal
					open
					slotKey={pickModalSlot}
					slotLabel={pickedField?.label ?? pickModalSlot}
					candidates={getCandidatesForSlotType(
						pickedField?.type ?? "",
						candidatesByType
					)}
					currentSelection={aiSelections[pickModalSlot]}
					onClose={() => setPickModalSlot(null)}
				/>
			)}
		</div>
	);
}
