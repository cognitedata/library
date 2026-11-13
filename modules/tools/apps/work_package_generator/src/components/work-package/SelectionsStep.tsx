import { Check, Sparkles } from "lucide-react";
import { useMemo, useState } from "react";
import { isBaseTemplateId, useAppConfig } from "@/app-config";
import { Badge } from "@/components/ui";
import type {
	AiSelections,
	ResourceInstance,
	TemplateField,
	WorkOrder,
} from "@/types";
import { PickResourceModal } from "./PickResourceModal";
import { AutomationReviewSection } from "./selections-step/AutomationReviewSection";
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
}: SelectionsStepProps) {
	const appConfig = useAppConfig();
	const [pickModalSlot, setPickModalSlot] = useState<string | null>(null);
	const isBaseTemplate = isBaseTemplateId(templateCanvasId, appConfig);

	const { filled, total, emptySlots } = useMemo(() => {
		const f = templateFields.filter((tf) => aiSelections[tf.label]).length;
		const empty = templateFields
			.filter((tf) => !aiSelections[tf.label])
			.map((tf) => tf.label);
		return { filled: f, total: templateFields.length, emptySlots: empty };
	}, [templateFields, aiSelections]);

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
			{total > 0 && (
				<div className="flex items-center gap-2 text-xs text-muted-foreground">
					<span>
						{filled} of {total} canvas slot{total !== 1 ? "s" : ""} filled
					</span>
					{filled === total && filled > 0 && (
						<Badge
							variant="success"
							className="text-[10px] px-1.5 py-0 bg-green-600 text-white hover:bg-green-600"
						>
							<Check className="h-2.5 w-2.5" />
							All filled
						</Badge>
					)}
					{filled > 0 && filled < total && (
						<Badge variant="secondary" className="text-[10px] px-1.5 py-0">
							Partial
						</Badge>
					)}
				</div>
			)}
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
							key={field.label}
							field={field}
							selection={aiSelections[field.label] ?? null}
							canPickManually={
								getCandidatesForSlotType(field.type, candidatesByType).length >
								0
							}
							isRunningAiSelection={isRunningAiSelection}
							onPickManually={setPickModalSlot}
						/>
					))}
				</div>
			)}

			{pickModalSlot && (
				<PickResourceModal
					open
					slotLabel={pickModalSlot}
					candidates={getCandidatesForSlotType(
						templateFields.find((f) => f.label === pickModalSlot)?.type ?? "",
						candidatesByType
					)}
					currentSelection={aiSelections[pickModalSlot]}
					onClose={() => setPickModalSlot(null)}
				/>
			)}
		</div>
	);
}
