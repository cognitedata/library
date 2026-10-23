import {
	Loader,
	Table,
	TableBody,
	TableCell,
	TableHead,
	TableHeader,
	TableRow,
} from "@/components/ui";
import type { WorkOrder } from "@/types";
import { cn, formatDate } from "@/utils";
import { WorkOrderResultsPagination } from "./work-order-search/WorkOrderResultsPagination";

export interface WorkOrderPaginationProps {
	hasNextPage: boolean;
	hasPrevPage: boolean;
	onNextPage: () => void;
	onPrevPage: () => void;
	currentPage: number;
	pageSize: number;
	isLoading?: boolean;
}

export interface WorkOrderResultsTableProps {
	rows: WorkOrder[];
	isConfirming: boolean;
	onSelect: (wo: WorkOrder) => void;
	emptyMessage?: string;
	pagination?: WorkOrderPaginationProps;
}

export function WorkOrderResultsTable({
	rows,
	isConfirming,
	onSelect,
	emptyMessage,
	pagination,
}: WorkOrderResultsTableProps) {
	if (rows.length === 0 && !pagination?.isLoading) {
		return (
			<div className="flex items-center justify-center py-8 text-xs text-muted-foreground">
				{emptyMessage ?? "No work orders found."}
			</div>
		);
	}

	const firstItem = pagination
		? (pagination.currentPage - 1) * pagination.pageSize + 1
		: 1;
	const lastItem = pagination ? firstItem + rows.length - 1 : rows.length;

	return (
		<div className="flex flex-col flex-1 min-h-0 gap-0 mt-2">
			<div className="flex-1 min-h-0 rounded-t-lg border border-b-0 overflow-x-auto overflow-y-auto">
				<Table className="min-w-[600px]">
					<TableHeader>
						<TableRow>
							<TableHead className="text-xs whitespace-nowrap sticky top-0 bg-background z-10">
								ID
							</TableHead>
							<TableHead className="text-xs whitespace-nowrap sticky top-0 bg-background z-10">
								Name
							</TableHead>
							<TableHead className="text-xs whitespace-nowrap sticky top-0 bg-background z-10">
								Status
							</TableHead>
							<TableHead className="text-xs whitespace-nowrap sticky top-0 bg-background z-10">
								Scheduled Start
							</TableHead>
							<TableHead className="text-xs whitespace-nowrap sticky top-0 bg-background z-10">
								Description
							</TableHead>
						</TableRow>
					</TableHeader>
					<TableBody>
						{rows.map((wo) => (
							<TableRow
								key={`${wo.space}/${wo.externalId}`}
								className={cn(
									"transition-colors",
									isConfirming
										? "cursor-not-allowed opacity-70"
										: "cursor-pointer hover:bg-muted/50"
								)}
								onClick={() => !isConfirming && onSelect(wo)}
							>
								<TableCell className="font-mono text-xs py-2 whitespace-nowrap">
									{isConfirming ? (
										<Loader size={12} className="inline mr-1.5" />
									) : null}
									{wo.externalId}
								</TableCell>
								<TableCell className="text-xs py-2 whitespace-nowrap">
									{wo.name ?? "—"}
								</TableCell>
								<TableCell className="text-xs py-2 whitespace-nowrap">
									{wo.status ? (
										<span className="inline-flex items-center rounded-full px-2 py-0.5 text-[10px] font-medium bg-muted text-muted-foreground">
											{wo.status}
										</span>
									) : (
										"—"
									)}
								</TableCell>
								<TableCell className="text-xs py-2 whitespace-nowrap text-muted-foreground">
									{formatDate(wo.scheduledStartTime) ?? "—"}
								</TableCell>
								<TableCell className="text-xs py-2 text-muted-foreground max-w-[200px] truncate">
									{wo.description ?? "—"}
								</TableCell>
							</TableRow>
						))}
					</TableBody>
				</Table>
			</div>

			{pagination && (
				<WorkOrderResultsPagination
					pagination={pagination}
					rowCount={rows.length}
					firstItem={firstItem}
					lastItem={lastItem}
				/>
			)}
		</div>
	);
}
