import { Plus, Search } from "lucide-react";
import {
	Button,
	Dialog,
	DialogContent,
	DialogFooter,
	DialogTitle,
	Input,
} from "@/components/ui";
import { useAddResourceModalViewModel } from "@/hooks/useAddResourceModalViewModel";
import type { ResourceConfig } from "@/types";
import { AddResourceResults } from "./add-resource/AddResourceResults";

export {
	type AddResourceModalDeps,
	AddResourceModalProvider,
} from "@/hooks/useAddResourceModalViewModel";

interface AddResourceModalProps {
	open: boolean;
	resourceType: string;
	displayName: string;
	onClose: () => void;
}

export function AddResourceModal({
	open,
	resourceType,
	displayName,
	onClose,
}: AddResourceModalProps) {
	const {
		searchText,
		setSearchText,
		debouncedQuery,
		results,
		isFetching,
		error,
		alreadyAdded,
		selectedItems,
		toggleSelection,
		handleAddSelected,
		selectedCount,
	} = useAddResourceModalViewModel({ open, resourceType, onClose });

	return (
		<Dialog open={open} onOpenChange={(isOpen) => !isOpen && onClose()}>
			<DialogContent className="max-w-lg p-4" closeButtonLabel="Close">
				<div className="mb-3 pr-8">
					<DialogTitle className="text-sm font-semibold">
						Add {displayName}
					</DialogTitle>
				</div>

				<div className="relative mb-3">
					<Search
						className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground"
						aria-hidden
					/>
					<Input
						value={searchText}
						onChange={(e) => setSearchText(e.target.value)}
						aria-label={`Search ${displayName}`}
						placeholder={`Search ${displayName.toLowerCase()}…`}
						className="pl-8 text-xs"
					/>
				</div>

				<AddResourceResults
					displayName={displayName}
					debouncedQuery={debouncedQuery}
					isFetching={isFetching}
					error={error}
					results={results}
					alreadyAdded={alreadyAdded}
					selectedItems={selectedItems}
					onToggle={toggleSelection}
				/>

				<DialogFooter className="mt-4">
					<Button type="button" variant="outline" size="sm" onClick={onClose}>
						Cancel
					</Button>
					<Button
						type="button"
						size="sm"
						disabled={selectedCount === 0}
						onClick={handleAddSelected}
					>
						<Plus className="h-3.5 w-3.5" />
						Add {selectedCount > 0 ? selectedCount : ""} selected
					</Button>
				</DialogFooter>
			</DialogContent>
		</Dialog>
	);
}

export type { ResourceConfig };
