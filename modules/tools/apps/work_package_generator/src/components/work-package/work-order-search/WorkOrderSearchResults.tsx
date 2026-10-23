import { RotateCcw } from "lucide-react";
import { Button, Loader } from "@/components/ui";
import type { WorkOrder } from "@/types";
import {
	type WorkOrderPaginationProps,
	WorkOrderResultsTable,
} from "../WorkOrderResultsTable";

export function buildWorkOrderPagination(input: {
	hasSearched: boolean;
	searchResultLimit: number;
	searchHasNextPage: boolean;
	searchHasPrevPage: boolean;
	onSearchNextPage?: () => void;
	onSearchPrevPage?: () => void;
	searchCurrentPage: number;
	isSearchFetching: boolean;
	recentHasNextPage: boolean;
	recentHasPrevPage: boolean;
	onRecentNextPage: () => void;
	onRecentPrevPage: () => void;
	recentCurrentPage: number;
	isFetchingRecent: boolean;
}): WorkOrderPaginationProps {
	if (input.hasSearched) {
		return {
			hasNextPage: input.searchHasNextPage,
			hasPrevPage: input.searchHasPrevPage,
			onNextPage: input.onSearchNextPage ?? (() => undefined),
			onPrevPage: input.onSearchPrevPage ?? (() => undefined),
			currentPage: input.searchCurrentPage,
			pageSize: input.searchResultLimit,
			isLoading: input.isSearchFetching,
		};
	}
	return {
		hasNextPage: input.recentHasNextPage,
		hasPrevPage: input.recentHasPrevPage,
		onNextPage: input.onRecentNextPage,
		onPrevPage: input.onRecentPrevPage,
		currentPage: input.recentCurrentPage,
		pageSize: input.searchResultLimit,
		isLoading: input.isFetchingRecent && input.recentCurrentPage > 1,
	};
}

export function WorkOrderSearchResults({
	hasSearched,
	isLoadingDisplay,
	searchErrorMessage,
	onRetrySearch,
	recentError,
	onRefetchRecent,
	displayRows,
	isConfirming,
	onSelect,
	hasActiveFilters,
	pagination,
}: {
	hasSearched: boolean;
	isLoadingDisplay: boolean;
	searchErrorMessage: string | null;
	onRetrySearch?: () => void;
	recentError: Error | null;
	onRefetchRecent: () => void;
	displayRows: WorkOrder[];
	isConfirming: boolean;
	onSelect: (wo: WorkOrder) => void;
	hasActiveFilters: boolean;
	pagination: WorkOrderPaginationProps;
}) {
	if (isLoadingDisplay) {
		return (
			<div className="flex items-center gap-2 py-6 text-xs text-muted-foreground">
				<Loader size={14} />
				{hasSearched ? "Searching..." : "Loading recent work orders..."}
			</div>
		);
	}

	if (hasSearched && searchErrorMessage) {
		return (
			<div className="flex flex-col gap-2 py-6 text-xs text-destructive">
				<p className="leading-snug">{searchErrorMessage}</p>
				{onRetrySearch && (
					<Button
						type="button"
						variant="outline"
						size="sm"
						onClick={onRetrySearch}
					>
						<RotateCcw className="h-3.5 w-3.5 shrink-0" />
						Retry search
					</Button>
				)}
			</div>
		);
	}

	if (!hasSearched && recentError) {
		return (
			<div className="flex items-center gap-2 py-6 text-xs text-destructive">
				<RotateCcw
					size={14}
					className="cursor-pointer shrink-0"
					onClick={() => onRefetchRecent()}
					aria-label="Retry"
				/>
				Failed to load recent work orders. Click to retry.
			</div>
		);
	}

	return (
		<WorkOrderResultsTable
			rows={displayRows}
			isConfirming={isConfirming}
			onSelect={onSelect}
			emptyMessage={
				hasSearched
					? "No work orders matched your search."
					: hasActiveFilters
						? "No work orders match the applied filters."
						: "No recent work orders found."
			}
			pagination={
				displayRows.length > 0 ||
				pagination.hasNextPage ||
				pagination.hasPrevPage
					? pagination
					: undefined
			}
		/>
	);
}
