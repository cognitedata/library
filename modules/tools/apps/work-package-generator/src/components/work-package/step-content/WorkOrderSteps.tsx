import type { WorkPackageGeneratorPageViewModel } from "@/hooks/useWorkPackageGeneratorPage";
import { WorkOrderDetailStep } from "../WorkOrderDetailStep";
import { WorkOrderSearchPanel } from "../WorkOrderSearchPanel";

export function WorkOrderSearchStep({
	vm,
}: {
	vm: WorkPackageGeneratorPageViewModel;
}) {
	return (
		<WorkOrderSearchPanel
			searchInput={vm.searchInput}
			onSearchInputChange={vm.setSearchInput}
			onSearch={vm.onSearch}
			isSearching={vm.isSearchingWorkOrders}
			isClientAvailable={!!vm.client}
			searchResults={vm.searchResults}
			hasSearched={vm.hasSearched}
			searchErrorMessage={vm.searchErrorMessage}
			onRetrySearch={vm.onRetrySearch}
			isConfirming={vm.isSelectingWorkOrder}
			onSelect={vm.selectWorkOrder}
			searchHasNextPage={vm.searchHasNextPage}
			searchHasPrevPage={vm.searchHasPrevPage}
			onSearchNextPage={vm.onSearchNextPage}
			onSearchPrevPage={vm.onSearchPrevPage}
			searchCurrentPage={vm.searchCurrentPage}
			isSearchFetching={vm.isSearchFetching}
			recentWorkOrders={vm.recentWorkOrders}
			isLoadingRecent={vm.isLoadingRecent}
			isFetchingRecent={vm.isFetchingRecent}
			recentError={vm.recentError}
			onRefetchRecent={vm.onRefetchRecent}
			recentHasNextPage={vm.recentHasNextPage}
			recentHasPrevPage={vm.recentHasPrevPage}
			onRecentNextPage={vm.onRecentNextPage}
			onRecentPrevPage={vm.onRecentPrevPage}
			recentCurrentPage={vm.recentCurrentPage}
			workOrderFilters={vm.workOrderFilters}
			onWorkOrderFiltersChange={vm.onWorkOrderFiltersChange}
		/>
	);
}

export function WorkOrderDetailStepPanel({
	vm,
}: {
	vm: WorkPackageGeneratorPageViewModel;
}) {
	if (!vm.workOrder) return null;
	return (
		<WorkOrderDetailStep
			workOrder={vm.workOrder}
			asset={vm.asset}
			searchInput={vm.searchInput}
			onSearchInputChange={vm.setSearchInput}
			onSearch={vm.onSearch}
			isSearching={vm.isFindingWorkOrder}
			isClientAvailable={!!vm.client}
			onGenerateWorkPackage={vm.onGenerateWorkPackage}
			onChangeWorkOrder={vm.onChangeWorkOrder}
		/>
	);
}
