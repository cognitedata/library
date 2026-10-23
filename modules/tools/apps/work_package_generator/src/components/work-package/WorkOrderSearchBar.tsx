/**
 * Work Order Search Bar Component
 * Search input with submit button for finding work orders
 */

import { Search } from "lucide-react";
import type { FormEvent } from "react";
import { useAppConfig } from "@/app-config";
import { Button, Input, Loader } from "@/components/ui";

interface WorkOrderSearchBarProps {
	searchInput: string;
	onSearchInputChange: (value: string) => void;
	onSearch: (e: FormEvent) => void;
	isSearching: boolean;
	isClientAvailable: boolean;
}

export function WorkOrderSearchBar({
	searchInput,
	onSearchInputChange,
	onSearch,
	isSearching,
	isClientAvailable,
}: WorkOrderSearchBarProps) {
	const workOrderTerm = useAppConfig().ui.terms.workOrder;
	const isButtonDisabled =
		isSearching || !searchInput.trim() || !isClientAvailable;

	const buttonContent = isSearching ? (
		<>
			<Loader size={16} />
			<span className="ml-2">Searching...</span>
		</>
	) : (
		"Find"
	);

	return (
		<div className="relative">
			<form onSubmit={onSearch}>
				<div className="flex flex-col gap-2">
					<div className="relative">
						<Search
							className="pointer-events-none absolute left-4 top-1/2 z-10 h-5 w-5 -translate-y-1/2 text-muted-foreground mobile:left-3"
							aria-hidden
						/>
						<Input
							type="text"
							value={searchInput}
							onChange={(e) => onSearchInputChange(e.target.value)}
							aria-label="Search work orders"
							placeholder={`Search by ${workOrderTerm} ID, name, or description...`}
							className="h-14 rounded-xl border-2 pl-12 pr-4 text-base mobile:pl-10 min-[817px]:pr-32"
							disabled={isSearching}
						/>
						<Button
							type="submit"
							disabled={isButtonDisabled}
							className="absolute right-2 top-1/2 hidden h-10 -translate-y-1/2 min-[817px]:inline-flex"
						>
							{buttonContent}
						</Button>
					</div>
					<Button
						type="submit"
						disabled={isButtonDisabled}
						className="h-10 w-full min-[817px]:hidden"
					>
						{buttonContent}
					</Button>
				</div>
			</form>
		</div>
	);
}
