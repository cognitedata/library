import type { WorkOrder, WorkOrderFilters } from "@/types";

export interface WorkOrderSearchPanelProps {
	searchInput: string;
	onSearchInputChange: (value: string) => void;
	onSearch: (e: React.FormEvent) => void;
	isSearching: boolean;
	isClientAvailable: boolean;
	searchResults: WorkOrder[];
	hasSearched: boolean;
	/** User-facing message when the search mutation failed */
	searchErrorMessage?: string | null;
	onRetrySearch?: () => void;
	isConfirming: boolean;
	onSelect: (wo: WorkOrder) => void;
	/** Search result pagination */
	searchHasNextPage?: boolean;
	searchHasPrevPage?: boolean;
	onSearchNextPage?: () => void;
	onSearchPrevPage?: () => void;
	searchCurrentPage?: number;
	isSearchFetching?: boolean;
	recentWorkOrders: WorkOrder[];
	isLoadingRecent: boolean;
	isFetchingRecent: boolean;
	recentError: Error | null;
	onRefetchRecent: () => void;
	recentHasNextPage: boolean;
	recentHasPrevPage: boolean;
	onRecentNextPage: () => void;
	onRecentPrevPage: () => void;
	recentCurrentPage: number;
	workOrderFilters: WorkOrderFilters;
	onWorkOrderFiltersChange: (filters: WorkOrderFilters) => void;
}
