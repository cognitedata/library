import { FileText } from "lucide-react";
import { useState } from "react";
import { useAppConfig } from "@/app-config";
import { WorkOrderFiltersSection } from "./WorkOrderFiltersSection";
import { WorkOrderSearchBar } from "./WorkOrderSearchBar";
import type { WorkOrderSearchPanelProps } from "./work-order-search/types";
import {
	buildWorkOrderPagination,
	WorkOrderSearchResults,
} from "./work-order-search/WorkOrderSearchResults";

export type { WorkOrderSearchPanelProps } from "./work-order-search/types";

export function WorkOrderSearchPanel({
	searchInput,
	onSearchInputChange,
	onSearch,
	isSearching,
	isClientAvailable,
	searchResults,
	hasSearched,
	searchErrorMessage = null,
	onRetrySearch,
	isConfirming,
	onSelect,
	searchHasNextPage = false,
	searchHasPrevPage = false,
	onSearchNextPage,
	onSearchPrevPage,
	searchCurrentPage = 1,
	isSearchFetching = false,
	recentWorkOrders,
	isLoadingRecent,
	isFetchingRecent,
	recentError,
	onRefetchRecent,
	recentHasNextPage,
	recentHasPrevPage,
	onRecentNextPage,
	onRecentPrevPage,
	recentCurrentPage,
	workOrderFilters,
	onWorkOrderFiltersChange,
}: WorkOrderSearchPanelProps) {
	const { limits, ui } = useAppConfig();
	const searchResultLimit = limits.searchResultLimit;
	const workOrderTerm = ui.terms.workOrder;
	const [showFilters, setShowFilters] = useState(false);

	const displayRows = hasSearched ? searchResults : recentWorkOrders;
	const isLoadingDisplay = hasSearched ? isSearching : isLoadingRecent;
	const hasActiveFilters =
		!hasSearched &&
		(!!workOrderFilters.dateFrom ||
			!!workOrderFilters.dateTo ||
			!!workOrderFilters.status ||
			!!workOrderFilters.priority);

	const pagination = buildWorkOrderPagination({
		hasSearched,
		searchResultLimit,
		searchHasNextPage,
		searchHasPrevPage,
		onSearchNextPage,
		onSearchPrevPage,
		searchCurrentPage,
		isSearchFetching,
		recentHasNextPage,
		recentHasPrevPage,
		onRecentNextPage,
		onRecentPrevPage,
		recentCurrentPage,
		isFetchingRecent,
	});

	return (
		<div
			className={`flex flex-col min-h-0 ${
				displayRows.length > 0 || isLoadingDisplay
					? "max-h-[70vh] shrink min-h-0"
					: "shrink-0"
			}`}
		>
			<div className="space-y-3 shrink-0">
				<h3 className="text-sm font-semibold flex items-center gap-2">
					<FileText className="h-4 w-4" />
					{workOrderTerm}
				</h3>
				<p className="text-xs text-muted-foreground">
					{hasSearched
						? "Search results — click a row to begin."
						: "Most recent work orders — click a row to begin, or search above."}
				</p>
				<WorkOrderSearchBar
					searchInput={searchInput}
					onSearchInputChange={onSearchInputChange}
					onSearch={onSearch}
					isSearching={isSearching}
					isClientAvailable={isClientAvailable}
				/>

				<WorkOrderFiltersSection
					filters={workOrderFilters}
					setFilters={(next) => {
						const resolved =
							typeof next === "function" ? next(workOrderFilters) : next;
						onWorkOrderFiltersChange(resolved);
					}}
					showFilters={showFilters}
					setShowFilters={setShowFilters}
					hasActiveFilters={hasActiveFilters}
					hasSearched={hasSearched}
					isFetchingRecent={isFetchingRecent}
					onRefetchRecent={onRefetchRecent}
				/>
			</div>

			<WorkOrderSearchResults
				hasSearched={hasSearched}
				isLoadingDisplay={isLoadingDisplay}
				searchErrorMessage={searchErrorMessage}
				onRetrySearch={onRetrySearch}
				recentError={recentError}
				onRefetchRecent={onRefetchRecent}
				displayRows={displayRows}
				isConfirming={isConfirming}
				onSelect={onSelect}
				hasActiveFilters={hasActiveFilters}
				pagination={pagination}
			/>
		</div>
	);
}
