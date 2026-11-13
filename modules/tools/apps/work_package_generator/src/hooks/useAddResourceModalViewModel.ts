import { useQuery } from "@tanstack/react-query";
import {
	createContext,
	createElement,
	type ReactNode,
	useCallback,
	useContext,
	useEffect,
	useMemo,
	useState,
} from "react";
import { searchResourcesByName } from "@/api/resources";
import { useAppConfig } from "@/app-config";
import { useCdfClientInfo } from "@/hooks/useClientInfo";
import { useWorkPackageStore } from "@/store";
import type { ResourceInstance } from "@/types";

export const SEARCH_DEBOUNCE_MS = 300;

export interface AddResourceModalDeps {
	useCdfClient: typeof useCdfClientInfo;
	useStore: typeof useWorkPackageStore;
	searchResources: typeof searchResourcesByName;
}

const defaultDeps: AddResourceModalDeps = {
	useCdfClient: useCdfClientInfo,
	useStore: useWorkPackageStore,
	searchResources: searchResourcesByName,
};

export const AddResourceModalContext =
	createContext<AddResourceModalDeps>(defaultDeps);

export function AddResourceModalProvider({
	children,
	value = defaultDeps,
}: {
	children: ReactNode;
	value?: AddResourceModalDeps;
}) {
	return createElement(AddResourceModalContext.Provider, { value }, children);
}

export type AddResourceModalViewModelInput = {
	open: boolean;
	resourceType: string;
	onClose: () => void;
};

export function useAddResourceModalViewModel({
	open,
	resourceType,
	onClose,
}: AddResourceModalViewModelInput) {
	const { useCdfClient, useStore, searchResources } = useContext(
		AddResourceModalContext
	);
	const { client } = useCdfClient();
	const appConfig = useAppConfig();
	const candidatesByType = useStore((s) => s.candidatesByType);
	const addCandidateToType = useStore((s) => s.addCandidateToType);

	const [searchText, setSearchText] = useState("");
	const [debouncedQuery, setDebouncedQuery] = useState("");
	const [selectedItems, setSelectedItems] = useState<
		Map<string, ResourceInstance>
	>(new Map());

	useEffect(() => {
		if (!open) {
			setSearchText("");
			setDebouncedQuery("");
			setSelectedItems(new Map());
		}
	}, [open]);

	useEffect(() => {
		const timer = setTimeout(
			() => setDebouncedQuery(searchText.trim()),
			SEARCH_DEBOUNCE_MS
		);
		return () => clearTimeout(timer);
	}, [searchText]);

	const alreadyAdded = useMemo(() => {
		const ids = new Set<string>();
		for (const item of candidatesByType[resourceType] ?? []) {
			ids.add(item.externalId);
		}
		return ids;
	}, [candidatesByType, resourceType]);

	const { data, isFetching, error } = useQuery({
		queryKey: ["addResourceSearch", resourceType, debouncedQuery],
		queryFn: async () => {
			if (!client) throw new Error("CDF client not available");
			return searchResources(client, debouncedQuery, resourceType, appConfig);
		},
		enabled: !!client && debouncedQuery.length > 0,
		staleTime: 30_000,
	});

	const toggleSelection = useCallback(
		(item: ResourceInstance, checked: boolean) => {
			setSelectedItems((prev) => {
				const next = new Map(prev);
				if (checked) next.set(item.externalId, item);
				else next.delete(item.externalId);
				return next;
			});
		},
		[]
	);

	const handleAddSelected = useCallback(() => {
		for (const item of selectedItems.values()) {
			addCandidateToType(resourceType, item);
		}
		onClose();
	}, [selectedItems, addCandidateToType, resourceType, onClose]);

	return {
		searchText,
		setSearchText,
		debouncedQuery,
		results: data ?? [],
		isFetching,
		error,
		alreadyAdded,
		selectedItems,
		toggleSelection,
		handleAddSelected,
		selectedCount: selectedItems.size,
	};
}
