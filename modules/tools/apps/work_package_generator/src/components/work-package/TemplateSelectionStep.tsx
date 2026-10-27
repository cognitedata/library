import { ArrowRight, RotateCcw, Sparkles } from "lucide-react";
import { TemplateCanvasInstructionsDialog } from "@/components/layout/TemplateCanvasInstructionsDialog";
import { Button, Loader } from "@/components/ui";
import type { TemplateSlot } from "@/hooks";
import type { TemplateCanvas } from "@/types";
import { TemplateCanvasCard } from "./template-selection-step/TemplateCanvasCard";
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
	onSelectTemplate,
	onContinueFromTemplate,
}: TemplateSelectionStepProps) {
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

			{isLoadingTemplates || isRunningAiCanvasRecommendation ? (
				<div className="flex items-center gap-2 text-xs text-muted-foreground py-8 justify-center">
					<Loader size={14} />
					{isRunningAiCanvasRecommendation
						? "AI analyzing templates..."
						: "Loading templates..."}
				</div>
			) : templates.length === 0 && templatesErrorMessage ? (
				<div
					className="flex flex-col items-center gap-3 py-8 px-3 text-center"
					role="alert"
				>
					<p className="text-xs text-destructive max-w-md leading-snug">
						{templatesErrorMessage}
					</p>
					{onRetryTemplates && (
						<Button
							type="button"
							variant="outline"
							size="sm"
							className="text-xs"
							onClick={onRetryTemplates}
						>
							<RotateCcw className="h-3.5 w-3.5 mr-1.5" />
							Retry
						</Button>
					)}
				</div>
			) : templates.length === 0 ? (
				<p className="text-xs text-muted-foreground py-8 text-center">
					No templates found
				</p>
			) : (
				<div className="grid grid-cols-2 gap-2 mobile:grid-cols-1 xl:grid-cols-3">
					{templates.map((t) => (
						<TemplateCanvasCard
							key={t.externalId}
							template={t}
							isSelected={templateCanvasId === t.externalId}
							isAiRecommended={t.externalId === aiSelectedCanvasId}
							slots={slotsByTemplate[t.externalId]}
							slotsLoading={!slotsByTemplate[t.externalId] && isLoadingSlots}
							onSelect={onSelectTemplate}
						/>
					))}
				</div>
			)}

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
