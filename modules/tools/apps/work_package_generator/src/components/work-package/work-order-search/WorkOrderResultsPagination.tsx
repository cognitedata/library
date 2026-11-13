import { ChevronLeft, ChevronRight } from "lucide-react";
import { Button, Loader } from "@/components/ui";
import type { WorkOrderPaginationProps } from "../WorkOrderResultsTable";

export function WorkOrderResultsPagination({
	pagination,
	rowCount,
	firstItem,
	lastItem,
}: {
	pagination: WorkOrderPaginationProps;
	rowCount: number;
	firstItem: number;
	lastItem: number;
}) {
	return (
		<div className="flex shrink-0 items-center justify-between rounded-b-lg border bg-muted/30 px-3 py-1.5">
			<span className="text-xs text-muted-foreground">
				{pagination.isLoading ? (
					<span className="inline-flex items-center gap-1.5">
						<Loader size={12} />
						Loading...
					</span>
				) : rowCount > 0 ? (
					`Showing ${firstItem}–${lastItem}`
				) : null}
			</span>
			<div className="flex items-center gap-1">
				<Button
					variant="ghost"
					size="sm"
					className="h-7 px-2 text-xs"
					disabled={!pagination.hasPrevPage || pagination.isLoading}
					onClick={pagination.onPrevPage}
					aria-label="Previous page"
				>
					<ChevronLeft className="h-3.5 w-3.5" />
					Prev
				</Button>
				<span className="min-w-12 text-center text-xs text-muted-foreground">
					Page {pagination.currentPage}
				</span>
				<Button
					variant="ghost"
					size="sm"
					className="h-7 px-2 text-xs"
					disabled={!pagination.hasNextPage || pagination.isLoading}
					onClick={pagination.onNextPage}
					aria-label="Next page"
				>
					Next
					<ChevronRight className="h-3.5 w-3.5" />
				</Button>
			</div>
		</div>
	);
}
