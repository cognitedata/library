import {
	createContext,
	type ReactNode,
	useCallback,
	useContext,
	useEffect,
	useState,
} from "react";
import {
	Button,
	Dialog,
	DialogContent,
	DialogFooter,
	DialogTitle,
} from "@/components/ui";
import { useWorkPackageStore } from "@/store";
import type { AiSelections, ResourceInstance } from "@/types";
import { PickResourceList } from "./pick-resource/PickResourceList";

export interface PickResourceModalDeps {
	useStore: typeof useWorkPackageStore;
}

const defaultDeps: PickResourceModalDeps = {
	useStore: useWorkPackageStore,
};

const PickResourceModalContext =
	createContext<PickResourceModalDeps>(defaultDeps);

interface PickResourceModalProps {
	open: boolean;
	slotLabel: string;
	candidates: ResourceInstance[];
	currentSelection: AiSelections[string];
	onClose: () => void;
}

export function PickResourceModalProvider({
	children,
	value = defaultDeps,
}: {
	children: ReactNode;
	value?: PickResourceModalDeps;
}) {
	return (
		<PickResourceModalContext.Provider value={value}>
			{children}
		</PickResourceModalContext.Provider>
	);
}

export function PickResourceModal({
	open,
	slotLabel,
	candidates,
	currentSelection,
	onClose,
}: PickResourceModalProps) {
	const { useStore } = useContext(PickResourceModalContext);
	const setSlotSelection = useStore((s) => s.setSlotSelection);

	const [filterText, setFilterText] = useState("");

	useEffect(() => {
		if (!open) setFilterText("");
	}, [open]);

	const handleSelect = useCallback(
		(item: ResourceInstance) => {
			const name =
				item.name ||
				(typeof item.properties?.name === "string"
					? item.properties.name
					: undefined);
			const description =
				item.description ||
				(typeof item.properties?.description === "string"
					? item.properties.description
					: undefined);

			setSlotSelection(slotLabel, {
				externalId: item.externalId,
				space: item.space,
				name,
				description,
			});
			onClose();
		},
		[setSlotSelection, slotLabel, onClose]
	);

	const handleClear = useCallback(() => {
		setSlotSelection(slotLabel, null);
		onClose();
	}, [setSlotSelection, slotLabel, onClose]);

	return (
		<Dialog open={open} onOpenChange={(isOpen) => !isOpen && onClose()}>
			<DialogContent className="max-w-lg p-4" closeButtonLabel="Close">
				<div className="mb-3 pr-8">
					<DialogTitle className="text-sm font-semibold">
						Pick resource for &ldquo;{slotLabel}&rdquo;
					</DialogTitle>
				</div>

				<PickResourceList
					candidates={candidates}
					filterText={filterText}
					onFilterTextChange={setFilterText}
					currentSelection={currentSelection}
					onSelect={handleSelect}
				/>

				<DialogFooter className="mt-4 sm:justify-between">
					<Button
						type="button"
						variant="outline"
						size="sm"
						onClick={handleClear}
						disabled={!currentSelection}
					>
						Clear selection
					</Button>
					<Button type="button" variant="outline" size="sm" onClick={onClose}>
						Cancel
					</Button>
				</DialogFooter>
			</DialogContent>
		</Dialog>
	);
}
