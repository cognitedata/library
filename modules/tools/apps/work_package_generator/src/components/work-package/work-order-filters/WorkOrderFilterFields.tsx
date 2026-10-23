import type { Dispatch, SetStateAction } from "react";
import { useAppConfig } from "@/app-config";
import {
	Input,
	Select,
	SelectContent,
	SelectItem,
	SelectTrigger,
	SelectValue,
} from "@/components/ui";
import type { WorkOrderFilters } from "@/types";

export function WorkOrderFilterFields({
	filters,
	setFilters,
	showFilters,
}: {
	filters: WorkOrderFilters;
	setFilters: Dispatch<SetStateAction<WorkOrderFilters>>;
	showFilters: boolean;
}) {
	const { ui } = useAppConfig();
	const statusOptions = ui.workOrderStatusOptions;
	const priorityOptions = ui.workOrderPriorityOptions;

	return (
		<section
			id="work-order-filters"
			aria-labelledby="work-order-filters-trigger"
			hidden={!showFilters}
			className="grid grid-cols-2 gap-2 p-3 rounded-lg border bg-muted/30 mobile:grid-cols-1"
		>
			<div className="flex flex-col gap-1">
				<label
					htmlFor="filter-date-from"
					className="text-[10px] font-medium text-muted-foreground uppercase tracking-wide"
				>
					Scheduled from
				</label>
				<Input
					id="filter-date-from"
					type="date"
					aria-label="Scheduled from (work order filter)"
					value={filters.dateFrom ?? ""}
					max={filters.dateTo}
					onChange={(e) =>
						setFilters((f) => ({
							...f,
							dateFrom: e.target.value || undefined,
						}))
					}
					className="h-8 px-2 text-xs"
				/>
			</div>

			<div className="flex flex-col gap-1">
				<label
					htmlFor="filter-date-to"
					className="text-[10px] font-medium text-muted-foreground uppercase tracking-wide"
				>
					Scheduled to
				</label>
				<Input
					id="filter-date-to"
					type="date"
					aria-label="Scheduled to (work order filter)"
					value={filters.dateTo ?? ""}
					min={filters.dateFrom}
					onChange={(e) =>
						setFilters((f) => ({
							...f,
							dateTo: e.target.value || undefined,
						}))
					}
					className="h-8 px-2 text-xs"
				/>
			</div>

			<div className="flex flex-col gap-1">
				<span className="text-[10px] font-medium text-muted-foreground uppercase tracking-wide">
					Status
				</span>
				<Select
					value={filters.status ?? ""}
					onValueChange={(val) =>
						setFilters((f) => ({
							...f,
							status: val === "__all__" ? undefined : val,
						}))
					}
				>
					<SelectTrigger className="h-8 text-xs">
						<SelectValue placeholder="Any status" />
					</SelectTrigger>
					<SelectContent>
						<SelectItem value="__all__">Any status</SelectItem>
						{statusOptions.map((s) => (
							<SelectItem key={s} value={s}>
								{s}
							</SelectItem>
						))}
					</SelectContent>
				</Select>
			</div>

			<div className="flex flex-col gap-1">
				<span className="text-[10px] font-medium text-muted-foreground uppercase tracking-wide">
					Priority
				</span>
				<Select
					value={filters.priority ?? ""}
					onValueChange={(val) =>
						setFilters((f) => ({
							...f,
							priority: val === "__all__" ? undefined : val,
						}))
					}
				>
					<SelectTrigger className="h-8 text-xs">
						<SelectValue placeholder="Any priority" />
					</SelectTrigger>
					<SelectContent>
						<SelectItem value="__all__">Any priority</SelectItem>
						{priorityOptions.map((p) => (
							<SelectItem key={p} value={p}>
								{p}
							</SelectItem>
						))}
					</SelectContent>
				</Select>
			</div>
		</section>
	);
}
