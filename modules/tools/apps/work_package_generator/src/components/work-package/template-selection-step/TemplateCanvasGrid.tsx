import { ChevronLeft, ChevronRight, RotateCcw } from "lucide-react";
import { useState } from "react";
import { Button, Loader } from "@/components/ui";
import type { TemplateSlot } from "@/hooks";
import { AI_RECOMMENDATION_FAILED_MESSAGE } from "@/hooks/useAiSelections";
import type { TemplateCanvas } from "@/types";
import { TemplateCanvasCard } from "./TemplateCanvasCard";

const TEMPLATES_PAGE_SIZE = 10;

/** All Resources stays first. The AI pick, when it is a different canvas, is second. */
function orderTemplatesWithAiPick(
	templates: TemplateCanvas[],
	aiSelectedCanvasId: string | null
): TemplateCanvas[] {
	if (!aiSelectedCanvasId || templates.length < 2) return templates;
	const aiIndex = templates.findIndex(
		(template) => template.externalId === aiSelectedCanvasId
	);
	if (aiIndex <= 1) return templates;
	const aiPick = templates[aiIndex];
	if (!aiPick) return templates;
	const rest = templates.filter(
		(template) => template.externalId !== aiSelectedCanvasId
	);
	const [first, ...tail] = rest;
	if (!first) return templates;
	return [first, aiPick, ...tail];
}

export function TemplateCanvasGrid({
	templates,
	templateCanvasId,
	aiSelectedCanvasId,
	slotsByTemplate,
	isLoadingSlots,
	isLoadingTemplates,
	templatesErrorMessage,
	onRetryTemplates,
	isRunningAiCanvasRecommendation,
	recommendationFailed,
	onRetryRecommendation,
	onSelectTemplate,
}: {
	templates: TemplateCanvas[];
	templateCanvasId: string;
	aiSelectedCanvasId: string | null;
	slotsByTemplate: Record<string, TemplateSlot[]>;
	isLoadingSlots: boolean;
	isLoadingTemplates: boolean;
	templatesErrorMessage: string | null;
	onRetryTemplates?: () => void;
	isRunningAiCanvasRecommendation: boolean;
	recommendationFailed: boolean;
	onRetryRecommendation?: () => void;
	onSelectTemplate: (id: string) => void;
}) {
	const orderedTemplates = orderTemplatesWithAiPick(
		templates,
		aiSelectedCanvasId
	);
	const pageCount = Math.max(
		1,
		Math.ceil(orderedTemplates.length / TEMPLATES_PAGE_SIZE)
	);
	const templateListKey = orderedTemplates
		.map((template) => template.externalId)
		.join("\n");
	const [page, setPage] = useState(1);
	const [pageListKey, setPageListKey] = useState(templateListKey);
	if (pageListKey !== templateListKey) {
		setPageListKey(templateListKey);
		setPage(1);
	}
	const currentPage = Math.min(page, pageCount);
	const pageStart = (currentPage - 1) * TEMPLATES_PAGE_SIZE;
	const visibleTemplates = orderedTemplates.slice(
		pageStart,
		pageStart + TEMPLATES_PAGE_SIZE
	);

	return (
		<>
			{recommendationFailed && !isRunningAiCanvasRecommendation && (
				<div className="flex flex-wrap items-center gap-2" role="alert">
					<p className="text-xs text-destructive">
						{AI_RECOMMENDATION_FAILED_MESSAGE}
					</p>
					{onRetryRecommendation && (
						<Button
							type="button"
							variant="outline"
							size="sm"
							className="text-xs"
							onClick={onRetryRecommendation}
						>
							<RotateCcw className="h-3.5 w-3.5 mr-1.5" />
							Retry
						</Button>
					)}
				</div>
			)}

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
				<div className="space-y-2">
					<div className="grid grid-cols-2 gap-2 mobile:grid-cols-1 xl:grid-cols-3">
						{visibleTemplates.map((t) => (
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
					{templates.length > TEMPLATES_PAGE_SIZE && (
						<div className="flex items-center justify-between gap-2">
							<span className="text-xs text-muted-foreground">
								Showing {pageStart + 1}–
								{Math.min(pageStart + TEMPLATES_PAGE_SIZE, templates.length)} of{" "}
								{templates.length}
							</span>
							<div className="flex items-center gap-1">
								<Button
									type="button"
									variant="ghost"
									size="sm"
									className="h-7 px-2 text-xs"
									disabled={currentPage <= 1}
									onClick={() => setPage(currentPage - 1)}
									aria-label="Previous page"
								>
									<ChevronLeft className="h-3.5 w-3.5" />
									Prev
								</Button>
								<span className="min-w-12 text-center text-xs text-muted-foreground">
									Page {currentPage}
								</span>
								<Button
									type="button"
									variant="ghost"
									size="sm"
									className="h-7 px-2 text-xs"
									disabled={currentPage >= pageCount}
									onClick={() => setPage(currentPage + 1)}
									aria-label="Next page"
								>
									Next
									<ChevronRight className="h-3.5 w-3.5" />
								</Button>
							</div>
						</div>
					)}
				</div>
			)}
		</>
	);
}
