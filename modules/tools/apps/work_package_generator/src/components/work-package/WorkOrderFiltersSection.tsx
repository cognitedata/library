import type { Dispatch, SetStateAction } from "react";
import { useMemo } from "react";
import type { WorkOrderFilters } from "@/types";
import { WorkOrderFilterFields } from "./work-order-filters/WorkOrderFilterFields";
import { WorkOrderFilterToolbar } from "./work-order-filters/WorkOrderFilterToolbar";

export interface WorkOrderFiltersSectionProps {
	filters: WorkOrderFilters;
	setFilters: Dispatch<SetStateAction<WorkOrderFilters>>;
	showFilters: boolean;
	setShowFilters: Dispatch<SetStateAction<boolean>>;
	hasActiveFilters: boolean;
	hasSearched: boolean;
	isFetchingRecent: boolean;
	onRefetchRecent: () => void;
}

export function WorkOrderFiltersSection({
	filters,
	setFilters,
	showFilters,
	setShowFilters,
	hasActiveFilters,
	hasSearched,
	isFetchingRecent,
	onRefetchRecent,
}: WorkOrderFiltersSectionProps) {
	const activeFiltersCount = useMemo(() => {
		return [
			filters.dateFrom,
			filters.dateTo,
			filters.status,
			filters.priority,
		].filter(Boolean).length;
	}, [filters.dateFrom, filters.dateTo, filters.status, filters.priority]);

	return (
		<>
			<WorkOrderFilterToolbar
				showFilters={showFilters}
				setShowFilters={setShowFilters}
				hasActiveFilters={hasActiveFilters}
				activeFiltersCount={activeFiltersCount}
				hasSearched={hasSearched}
				isFetchingRecent={isFetchingRecent}
				onRefetchRecent={onRefetchRecent}
				onClearFilters={() => setFilters({})}
			/>
			<WorkOrderFilterFields
				filters={filters}
				setFilters={setFilters}
				showFilters={showFilters}
			/>
		</>
	);
}
