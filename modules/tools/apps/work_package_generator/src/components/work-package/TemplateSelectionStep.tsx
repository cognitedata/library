import { ArrowRight, Sparkles } from "lucide-react";
import { TemplateCanvasInstructionsDialog } from "@/components/layout/TemplateCanvasInstructionsDialog";
import { Button } from "@/components/ui";
import type { TemplateSlot } from "@/hooks";
import { useAiRunUi } from "@/hooks/useAiSelections";
import type { TemplateCanvas } from "@/types";
import { TemplateCanvasGrid } from "./template-selection-step/TemplateCanvasGrid";
import { TemplateSelectionAlerts } from "./template-selection-step/TemplateSelectionAlerts";

interface TemplateSelectionStepProps {
	templates: TemplateCanvas[];
	templateCanvasId: string;
	aiSelectedCanvasId: string | null;
	slotsByTemplate: Record<string, TemplateSlot[]>;
	isLoadingSlots: boolean;
	slotsError?: boolean;
	onRetrySlots?: () => void;
	isLoadingTemplates: boolean;
	templatesErrorMessage?: string | null;
	onRetryTemplates?: () => void;
	isRunningAiCanvasRecommendation: boolean;
	aiRecommendationFailed?: boolean;
	onRetryRecommendation?: () => void;
	onSelectTemplate: (id: string) => void;
	onContinueFromTemplate: () => void;
}

export function TemplateSelectionStep({
	templates,
	templateCanvasId,
	aiSelectedCanvasId,
	slotsByTemplate,
	isLoadingSlots,
	slotsError = false,
	onRetrySlots,
	isLoadingTemplates,
	templatesErrorMessage = null,
	onRetryTemplates,
	isRunningAiCanvasRecommendation,
	aiRecommendationFailed = false,
	onRetryRecommendation,
	onSelectTemplate,
	onContinueFromTemplate,
}: TemplateSelectionStepProps) {
	const runUi = useAiRunUi();
	const recommendationFailed =
		aiRecommendationFailed || runUi.recommendationFailed;
	const retryRecommendation =
		onRetryRecommendation ?? runUi.rerunRecommendation ?? undefined;
	const isContinueDisabled =
		!templateCanvasId || isLoadingTemplates || isRunningAiCanvasRecommendation;
	const continueHint = !templateCanvasId
		? "Select a template to continue."
		: null;

	return (
		<div className="space-y-3">
			<TemplateSelectionAlerts
				templatesCount={templates.length}
				templatesErrorMessage={templatesErrorMessage}
				onRetryTemplates={onRetryTemplates}
				slotsError={slotsError}
				onRetrySlots={onRetrySlots}
			/>
			<div className="flex items-center justify-between gap-2">
				<div className="flex min-w-0 items-center gap-1">
					<h3 className="text-sm font-semibold flex items-center gap-2">
						<Sparkles className="h-4 w-4" />
						Template Canvas
					</h3>
					<TemplateCanvasInstructionsDialog />
				</div>
				{templates.length > 0 && (
					<span className="text-xs text-muted-foreground">
						{templates.length} template{templates.length !== 1 ? "s" : ""}
					</span>
				)}
			</div>

			<TemplateCanvasGrid
				templates={templates}
				templateCanvasId={templateCanvasId}
				aiSelectedCanvasId={aiSelectedCanvasId}
				slotsByTemplate={slotsByTemplate}
				isLoadingSlots={isLoadingSlots}
				isLoadingTemplates={isLoadingTemplates}
				templatesErrorMessage={templatesErrorMessage}
				onRetryTemplates={onRetryTemplates}
				isRunningAiCanvasRecommendation={isRunningAiCanvasRecommendation}
				recommendationFailed={recommendationFailed}
				onRetryRecommendation={retryRecommendation}
				onSelectTemplate={onSelectTemplate}
			/>

			{templates.length > 0 &&
				!isLoadingTemplates &&
				!isRunningAiCanvasRecommendation && (
					<div className="border-border border-t pt-4 space-y-3">
						<Button
							type="button"
							size="lg"
							className="w-full mobile:w-full sm:w-auto"
							disabled={isContinueDisabled}
							onClick={onContinueFromTemplate}
						>
							<ArrowRight className="size-4" aria-hidden />
							Continue to resources
						</Button>
						{continueHint && (
							<p className="text-xs text-muted-foreground">{continueHint}</p>
						)}
					</div>
				)}
		</div>
	);
}
