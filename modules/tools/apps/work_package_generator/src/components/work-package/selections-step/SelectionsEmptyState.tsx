import { RotateCcw } from "lucide-react";
import { Button, Loader } from "@/components/ui";
import type { WorkOrder } from "@/types";
import { SEARCH_WORK_ORDER_TO_BEGIN } from "@/utils/workflow-copy";

interface SelectionsEmptyStateProps {
	workOrder: WorkOrder | null;
	templateCanvasId: string | null;
	isBaseTemplate: boolean;
	isLoadingFields: boolean;
	templateFieldsErrorMessage: string | null;
	onRetryTemplateFields?: () => void;
}

export function SelectionsEmptyState({
	workOrder,
	templateCanvasId,
	isBaseTemplate,
	isLoadingFields,
	templateFieldsErrorMessage,
	onRetryTemplateFields,
}: SelectionsEmptyStateProps) {
	return (
		<div className="flex flex-col items-center justify-center gap-1 py-6 px-3 text-center text-xs text-muted-foreground border rounded-md bg-muted/10">
			{isBaseTemplate ? (
				<p className="max-w-md leading-snug">
					The All Resources template places every collected resource on the
					canvas automatically. You can continue to Create.
				</p>
			) : isLoadingFields ? (
				<>
					<Loader size={18} className="shrink-0" />
					<span>Loading slots from your template…</span>
				</>
			) : templateCanvasId && templateFieldsErrorMessage ? (
				<div className="flex flex-col items-center gap-3 max-w-md" role="alert">
					<p className="text-destructive leading-snug">
						{templateFieldsErrorMessage}
					</p>
					{onRetryTemplateFields && (
						<Button
							type="button"
							variant="outline"
							size="sm"
							className="text-xs border-destructive/40 text-destructive hover:bg-destructive/10"
							onClick={onRetryTemplateFields}
						>
							<RotateCcw className="h-3.5 w-3.5 mr-1.5" />
							Retry
						</Button>
					)}
				</div>
			) : !workOrder ? (
				<div className="space-y-1.5 max-w-md text-center">
					<p className="font-medium text-foreground">
						{SEARCH_WORK_ORDER_TO_BEGIN}
					</p>
					<p className="leading-snug">
						Resource selections are generated automatically once you continue
						with AI suggestions and pick a template.
					</p>
				</div>
			) : !templateCanvasId ? (
				<p className="max-w-md leading-snug">
					Choose a template in the Template step. Resource selections for each
					canvas slot are filled automatically from your work order and asset.
				</p>
			) : (
				<p className="max-w-md leading-snug">
					This template has no resource slots to fill. You can continue to
					Create.
				</p>
			)}
		</div>
	);
}
