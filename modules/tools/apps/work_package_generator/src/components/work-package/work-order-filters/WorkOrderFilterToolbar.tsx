import { RotateCcw, SlidersHorizontal } from "lucide-react";
import type { Dispatch, SetStateAction } from "react";
import { Button } from "@/components/ui";
import { cn } from "@/utils";

export function WorkOrderFilterToolbar({
	showFilters,
	setShowFilters,
	hasActiveFilters,
	activeFiltersCount,
	hasSearched,
	isFetchingRecent,
	onRefetchRecent,
	onClearFilters,
}: {
	showFilters: boolean;
	setShowFilters: Dispatch<SetStateAction<boolean>>;
	hasActiveFilters: boolean;
	activeFiltersCount: number;
	hasSearched: boolean;
	isFetchingRecent: boolean;
	onRefetchRecent: () => void;
	onClearFilters: () => void;
}) {
	return (
		<div className="flex items-center gap-2 flex-wrap">
			<Button
				id="work-order-filters-trigger"
				type="button"
				size="sm"
				variant={showFilters || hasActiveFilters ? "secondary" : "outline"}
				onClick={() => setShowFilters((v) => !v)}
				aria-expanded={showFilters}
				aria-controls="work-order-filters"
			>
				<SlidersHorizontal className="h-3 w-3" />
				Filters
				{hasActiveFilters && (
					<span className="ml-0.5 rounded-full bg-primary text-primary-foreground text-[10px] px-1.5 py-0 leading-4">
						{activeFiltersCount}
					</span>
				)}
			</Button>

			{hasActiveFilters && (
				<Button
					type="button"
					size="sm"
					variant="ghost"
					onClick={onClearFilters}
				>
					<RotateCcw className="h-3 w-3" />
					Clear
				</Button>
			)}

			{!hasSearched && (
				<Button
					type="button"
					size="sm"
					variant="ghost"
					className="ml-auto"
					onClick={() => onRefetchRecent()}
					disabled={isFetchingRecent}
					aria-busy={isFetchingRecent}
					aria-label={
						isFetchingRecent
							? "Refreshing work orders"
							: "Refresh work orders list"
					}
				>
					<RotateCcw
						className={cn(
							"h-3.5 w-3.5 shrink-0",
							isFetchingRecent && "animate-spin"
						)}
					/>
					{isFetchingRecent ? "Refreshing…" : "Refresh"}
				</Button>
			)}
		</div>
	);
}
