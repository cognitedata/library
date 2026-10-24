import { Activity, ArrowRight, Check, FileText, X } from "lucide-react";
import { Badge, Button, Loader } from "@/components/ui";
import type { TemplateSlot } from "@/hooks";
import type { Selection, TemplateField } from "@/types";
import { slotCategoryLabel } from "../slot-helpers";

interface SelectionSlotCardProps {
	field: TemplateField;
	selection: Selection | null;
	canPickManually: boolean;
	isRunningAiSelection: boolean;
	onPickManually: (slotLabel: string) => void;
}

export function SelectionSlotCard({
	field,
	selection,
	canPickManually,
	isRunningAiSelection,
	onPickManually,
}: SelectionSlotCardProps) {
	const hasSelection = selection != null;
	return (
		<div
			className={`rounded-lg border p-3 transition-colors ${
				hasSelection
					? "border-green-200 bg-green-50/50 dark:border-green-900 dark:bg-green-950/30"
					: isRunningAiSelection
						? "border-border bg-muted/20"
						: "border-destructive/30 bg-destructive/5"
			}`}
		>
			<div className="flex items-center gap-2 mb-2">
				<div className="shrink-0">
					{hasSelection ? (
						<div className="h-5 w-5 rounded-full bg-green-600 flex items-center justify-center">
							<Check className="h-3 w-3 text-white" />
						</div>
					) : isRunningAiSelection ? (
						<Loader size={18} />
					) : (
						<div className="h-5 w-5 rounded-full bg-muted flex items-center justify-center">
							<X className="h-3 w-3 text-muted-foreground" />
						</div>
					)}
				</div>
				<Badge
					variant="gray"
					outline
					className="text-[10px] px-1.5 py-0 capitalize"
				>
					{field.type === "file" ? (
						<FileText className="h-2.5 w-2.5" />
					) : (
						<Activity className="h-2.5 w-2.5" />
					)}
					{slotCategoryLabel(field.type as TemplateSlot["category"])}
				</Badge>
			</div>
			<div className="ml-7 flex items-stretch gap-2 mobile:ml-0 mobile:flex-col mobile:gap-2">
				<div className="flex-1 min-w-0 rounded-md bg-muted/40 border border-dashed border-muted-foreground/20 px-2.5 py-1.5">
					<p className="text-[10px] text-muted-foreground uppercase tracking-wider mb-0.5">
						Canvas slot
					</p>
					<p className="text-xs font-medium truncate">{field.label}</p>
				</div>
				<div className="flex shrink-0 items-center justify-center mobile:hidden">
					<ArrowRight className="h-3.5 w-3.5 text-muted-foreground" />
				</div>
				<div
					className={`flex-1 min-w-0 rounded-md px-2.5 py-1.5 ${
						hasSelection
							? "bg-green-100/50 dark:bg-green-950/40 border border-green-200 dark:border-green-900"
							: "bg-muted/20 border border-dashed border-muted-foreground/20"
					}`}
				>
					<div className="flex items-center justify-between gap-1.5 min-w-0 mb-0.5">
						<p className="text-[10px] text-muted-foreground uppercase tracking-wider truncate min-w-0">
							Selected resource
						</p>
						{canPickManually && (
							<Button
								type="button"
								variant="outline"
								size="sm"
								onClick={() => onPickManually(field.label)}
							>
								Pick manually
							</Button>
						)}
					</div>
					{hasSelection ? (
						<>
							<p className="text-xs font-medium truncate">
								{selection.name || selection.externalId}
							</p>
							{selection.name && (
								<p className="text-[10px] font-mono text-muted-foreground truncate">
									{selection.externalId}
								</p>
							)}
							{selection.description && (
								<p className="text-[10px] text-muted-foreground line-clamp-1 mt-0.5">
									{selection.description}
								</p>
							)}
						</>
					) : (
						<div className="text-xs text-muted-foreground space-y-0.5">
							{isRunningAiSelection ? (
								<p className="italic">Selecting...</p>
							) : (
								<>
									<p className="font-medium not-italic">No automatic match</p>
									<p className="text-[11px] leading-snug">
										Go to Resources to review data, run AI selection again from
										there if needed, then return here.
									</p>
								</>
							)}
						</div>
					)}
				</div>
			</div>
		</div>
	);
}
