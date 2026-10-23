import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook, waitFor } from "@testing-library/react";
import { createElement, type ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { AppConfigProvider } from "@/app-config";
import type { ResourceInstance } from "@/types";
import {
	type AddResourceModalDeps,
	AddResourceModalProvider,
	SEARCH_DEBOUNCE_MS,
	useAddResourceModalViewModel,
} from "./useAddResourceModalViewModel";

function makeWrapper(deps: AddResourceModalDeps) {
	const queryClient = new QueryClient({
		defaultOptions: { queries: { retry: false } },
	});
	return ({ children }: { children: ReactNode }) =>
		createElement(
			AppConfigProvider,
			null,
			createElement(
				QueryClientProvider,
				{ client: queryClient },
				createElement(AddResourceModalProvider, { value: deps, children })
			)
		);
}

describe(useAddResourceModalViewModel.name, () => {
	const searchResult: ResourceInstance = {
		externalId: "new-file",
		space: "s",
		name: "New File",
		properties: {},
	};

	let addCandidateToType: ReturnType<typeof vi.fn>;
	let deps: AddResourceModalDeps;

	beforeEach(() => {
		addCandidateToType = vi.fn();
		deps = {
			useCdfClient: () => ({ client: {} as never, project: "p", cluster: "c" }),
			useStore: ((selector: (state: unknown) => unknown) =>
				selector({
					candidatesByType: {
						file: [{ externalId: "existing", space: "s", properties: {} }],
					},
					addCandidateToType,
				})) as AddResourceModalDeps["useStore"],
			searchResources: vi.fn().mockResolvedValue([searchResult]),
		};
	});

	it("should stay idle until the search query is debounced", () => {
		const { result } = renderHook(
			() =>
				useAddResourceModalViewModel({
					open: true,
					resourceType: "file",
					onClose: vi.fn(),
				}),
			{ wrapper: makeWrapper(deps) }
		);

		act(() => {
			result.current.setSearchText("new");
		});

		expect(result.current.debouncedQuery).toBe("");
		expect(result.current.results).toEqual([]);
		expect(deps.searchResources).not.toHaveBeenCalled();
	});

	it("should return search results after debounce", async () => {
		const { result } = renderHook(
			() =>
				useAddResourceModalViewModel({
					open: true,
					resourceType: "file",
					onClose: vi.fn(),
				}),
			{ wrapper: makeWrapper(deps) }
		);

		act(() => {
			result.current.setSearchText("new");
		});

		await waitFor(
			() => {
				expect(result.current.results).toEqual([searchResult]);
			},
			{ timeout: SEARCH_DEBOUNCE_MS + 2000 }
		);
		expect(result.current.isFetching).toBe(false);
		expect(result.current.error).toBeNull();
	});

	it("should expose search errors", async () => {
		deps.searchResources = vi.fn().mockRejectedValue(new Error("boom"));
		const { result } = renderHook(
			() =>
				useAddResourceModalViewModel({
					open: true,
					resourceType: "file",
					onClose: vi.fn(),
				}),
			{ wrapper: makeWrapper(deps) }
		);

		act(() => {
			result.current.setSearchText("err");
		});

		await waitFor(
			() => {
				expect(result.current.error).toBeTruthy();
			},
			{ timeout: SEARCH_DEBOUNCE_MS + 2000 }
		);
	});

	it("should add selected items and close", () => {
		const onClose = vi.fn();
		const { result } = renderHook(
			() =>
				useAddResourceModalViewModel({
					open: true,
					resourceType: "file",
					onClose,
				}),
			{ wrapper: makeWrapper(deps) }
		);

		act(() => {
			result.current.toggleSelection(searchResult, true);
		});
		act(() => {
			result.current.handleAddSelected();
		});

		expect(addCandidateToType).toHaveBeenCalledWith("file", searchResult);
		expect(onClose).toHaveBeenCalled();
	});

	it("should mark already-added candidates", () => {
		const { result } = renderHook(
			() =>
				useAddResourceModalViewModel({
					open: true,
					resourceType: "file",
					onClose: vi.fn(),
				}),
			{ wrapper: makeWrapper(deps) }
		);

		expect(result.current.alreadyAdded.has("existing")).toBe(true);
		expect(result.current.alreadyAdded.has("new-file")).toBe(false);
	});
});
