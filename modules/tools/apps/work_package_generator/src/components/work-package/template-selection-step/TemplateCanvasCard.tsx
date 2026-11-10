import { Check, Sparkles } from "lucide-react";
import { useSyncExternalStore } from "react";
import {
	getTemplateReadNotices,
	subscribeTemplateReadNotices,
} from "@/api/retrieve-data/template-canvases";
import { Badge, Loader } from "@/components/ui";
import type { TemplateSlot } from "@/hooks";
import type { TemplateCanvas } from "@/types";
import { SlotIcon } from "../slot-helpers";

interface TemplateCanvasCardProps {
	template: TemplateCanvas;
	isSelected: boolean;
	isAiRecommended: boolean;
	slots: TemplateSlot[] | undefined;
	slotsLoading: boolean;
	onSelect: (id: string) => void;
}

export function TemplateCanvasCard({
	template,
	isSelected,
	isAiRecommended,
	slots,
	slotsLoading,
	onSelect,
}: TemplateCanvasCardProps) {
	const notices = useSyncExternalStore(
		subscribeTemplateReadNotices,
		getTemplateReadNotices,
		getTemplateReadNotices
	);
	const childTruncated = notices.truncatedChildIds.includes(
		template.externalId
	);
	const accessibleName = [
		template.name,
		isAiRecommended ? "AI pick" : undefined,
	]
		.filter(Boolean)
		.join(", ");

	return (
		<button
			type="button"
			aria-pressed={isSelected}
			aria-label={accessibleName}
			onClick={() => onSelect(template.externalId)}
			className={`relative text-left rounded-lg border-2 p-3 transition-all flex flex-col gap-2 ${
				isSelected
					? "border-primary bg-primary/5 ring-1 ring-primary/20"
					: "border-border hover:border-muted-foreground/30 hover:bg-muted/30"
			}`}
		>
			<div className="flex items-start justify-between gap-1.5">
				<span className="text-xs font-semibold leading-snug line-clamp-2 flex-1 min-w-0">
					{template.name}
				</span>
				<div
					className={`mt-0.5 h-4 w-4 rounded-full border-2 shrink-0 flex items-center justify-center transition-colors ${
						isSelected
							? "border-primary bg-primary"
							: "border-muted-foreground/30"
					}`}
				>
					{isSelected && (
						<Check className="h-2.5 w-2.5 text-primary-foreground" />
					)}
				</div>
			</div>

			<p className="text-xs text-neutral-800 dark:text-neutral-100">
				{template.updatedAt
					? `Updated ${template.updatedAt}`
					: "Updated date unknown"}
				{template.createdBy ? ` · owner ${template.createdBy}` : ""}
			</p>

			{(notices.catalogTruncated || childTruncated) && (
				<p
					role="status"
					className="text-xs text-neutral-900 dark:text-neutral-50"
				>
					{childTruncated
						? "This template was cut off at 5,000 items."
						: "The template list was cut off. Some templates are not shown."}
				</p>
			)}

			{(isAiRecommended || isSelected) && (
				<div className="flex gap-1 flex-wrap">
					{isAiRecommended && (
						<Badge
							variant="secondary"
							className="text-xs px-1.5 py-0 bg-blue-800 text-white hover:bg-blue-800 dark:bg-blue-200 dark:text-blue-950"
						>
							<Sparkles className="h-2.5 w-2.5 mr-0.5" />
							AI Pick
						</Badge>
					)}
					{isSelected && !isAiRecommended && (
						<Badge
							variant="success"
							className="text-xs px-1.5 py-0 bg-emerald-800 text-white hover:bg-emerald-800 dark:bg-emerald-200 dark:text-emerald-950"
						>
							<Check className="h-2.5 w-2.5 mr-0.5" />
							Selected
						</Badge>
					)}
				</div>
			)}

			{slotsLoading ? (
				<div className="flex items-center gap-1 text-xs text-neutral-800 dark:text-neutral-100">
					<Loader size={10} />
					Loading...
				</div>
			) : slots && slots.length > 0 ? (
				<div className="space-y-0.5 border-t border-muted/40 pt-2 mt-auto">
					{slots.map((slot) => (
						<div
							key={`${slot.viewExternalId}:${slot.label}:${slot.category}:${slot.populatedBy}`}
							className="flex items-center gap-1 text-xs"
						>
							<SlotIcon category={slot.category} />
							<span className="text-neutral-800 dark:text-neutral-100 truncate flex-1 min-w-0">
								{slot.label}
							</span>
							<Badge
								variant="gray"
								outline
								className="text-xs px-1 py-0 shrink-0 bg-neutral-200 text-neutral-900 border-neutral-500 dark:bg-neutral-200 dark:text-neutral-950"
							>
								{slot.populatedBy === "auto" ? "auto" : "AI"}
							</Badge>
						</div>
					))}
				</div>
			) : slots && slots.length === 0 ? (
				<p className="text-xs text-neutral-800 dark:text-neutral-100 mt-auto border-t border-muted/40 pt-2">
					No slots
				</p>
			) : null}
		</button>
	);
}
