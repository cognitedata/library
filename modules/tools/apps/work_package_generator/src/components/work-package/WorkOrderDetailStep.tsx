import { FileText, Package, Undo2 } from "lucide-react";
import { useAppConfig } from "@/app-config";
import { Button } from "@/components/ui";
import type { Asset, WorkOrder } from "@/types";
import { WorkOrderSearchBar } from "./WorkOrderSearchBar";
import { TargetAssetPanel } from "./work-order-detail/TargetAssetPanel";
import { WorkOrderFields } from "./work-order-detail/WorkOrderFields";

interface WorkOrderDetailStepProps {
	workOrder: WorkOrder;
	asset: Asset | null;
	searchInput: string;
	onSearchInputChange: (value: string) => void;
	onSearch: (e: React.FormEvent) => void;
	isSearching: boolean;
	isClientAvailable: boolean;
	/** Continue to template selection and the rest of the workflow. */
	onGenerateWorkPackage: () => void;
	/** Reset workflow and return to work order search. */
	onChangeWorkOrder?: () => void;
	/** Error message when asset resolution fails */
	assetErrorMessage?: string | null;
	/** Retry callback for asset resolution */
	onRetryAssetResolution?: () => void;
}

export function WorkOrderDetailStep({
	workOrder,
	asset,
	searchInput,
	onSearchInputChange,
	onSearch,
	isSearching,
	isClientAvailable,
	onGenerateWorkPackage,
	onChangeWorkOrder,
	assetErrorMessage = null,
	onRetryAssetResolution,
}: WorkOrderDetailStepProps) {
	const workOrderTerm = useAppConfig().ui.terms.workOrder;
	const isGenerateDisabled = isSearching || !asset || !!assetErrorMessage;
	const disabledHint = isSearching
		? "Loading work order details…"
		: assetErrorMessage
			? "Asset resolution failed"
			: !asset
				? "Resolving target asset…"
				: null;

	return (
		<div className="space-y-4">
			<div className="flex items-center justify-between gap-2">
				<div className="flex items-center gap-2">
					<FileText className="h-4 w-4 shrink-0 text-muted-foreground" />
					<span className="text-sm font-semibold">{workOrderTerm} & Asset</span>
				</div>
				{onChangeWorkOrder && (
					<Button
						type="button"
						variant="outline"
						size="sm"
						className="text-xs shrink-0"
						onClick={onChangeWorkOrder}
					>
						<Undo2 className="h-3.5 w-3.5" />
						Change work order
					</Button>
				)}
			</div>

			<WorkOrderSearchBar
				searchInput={searchInput}
				onSearchInputChange={onSearchInputChange}
				onSearch={onSearch}
				isSearching={isSearching}
				isClientAvailable={isClientAvailable}
			/>

			<div className="grid grid-cols-2 gap-6 mobile:grid-cols-1 mobile:gap-4">
				<WorkOrderFields workOrder={workOrder} />
				<TargetAssetPanel
					asset={asset}
					assetErrorMessage={assetErrorMessage}
					onRetryAssetResolution={onRetryAssetResolution}
				/>
			</div>

			<div className="border-border border-t pt-4 space-y-3">
				<div className="rounded-lg border border-border bg-muted/30 px-3 py-2.5 text-xs text-muted-foreground leading-relaxed">
					<p>
						When you continue, the app will{" "}
						<strong className="text-foreground">automatically</strong> recommend
						a canvas template and suggest resources for each slot. Nothing is
						saved to Fusion until you create or update the canvas on the last
						step.
					</p>
					<p className="mt-2">
						You can{" "}
						<strong className="text-foreground">review and change</strong> the
						template, resources, and selections on each step before then.
					</p>
				</div>
				<Button
					type="button"
					size="lg"
					className="w-full mobile:w-full sm:w-auto"
					disabled={isGenerateDisabled}
					onClick={onGenerateWorkPackage}
				>
					<Package className="size-4" aria-hidden />
					Continue with AI suggestions
				</Button>
				{disabledHint && (
					<p className="mt-2 text-xs text-muted-foreground">{disabledHint}</p>
				)}
			</div>
		</div>
	);
}
