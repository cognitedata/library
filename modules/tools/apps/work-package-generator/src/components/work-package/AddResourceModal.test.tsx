import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { AppConfigProvider } from "@/app-config";
import type { ResourceInstance } from "@/types";
import {
	AddResourceModal,
	type AddResourceModalDeps,
	AddResourceModalProvider,
} from "./AddResourceModal";

function wrapper(deps: AddResourceModalDeps) {
	const client = new QueryClient({
		defaultOptions: { queries: { retry: false } },
	});
	return ({ children }: { children: React.ReactNode }) => (
		<AppConfigProvider>
			<QueryClientProvider client={client}>
				<AddResourceModalProvider value={deps}>
					{children}
				</AddResourceModalProvider>
			</QueryClientProvider>
		</AppConfigProvider>
	);
}

describe("AddResourceModal", () => {
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

	it("prompts for a search before a query is entered", () => {
		render(
			<AddResourceModal
				open
				resourceType="file"
				displayName="Files"
				onClose={vi.fn()}
			/>,
			{ wrapper: wrapper(deps) }
		);
		expect(screen.getByText(/type to search for files/i)).toBeInTheDocument();
		expect(
			screen.getByRole("button", { name: /add\s+selected/i })
		).toBeDisabled();
	});

	it("closes from Cancel and the dialog close control", () => {
		const onClose = vi.fn();
		render(
			<AddResourceModal
				open
				resourceType="file"
				displayName="Files"
				onClose={onClose}
			/>,
			{ wrapper: wrapper(deps) }
		);
		fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
		fireEvent.click(screen.getByRole("button", { name: "Close" }));
		expect(onClose).toHaveBeenCalledTimes(2);
	});

	it("searches, selects, adds, and closes", async () => {
		const onClose = vi.fn();
		render(
			<AddResourceModal
				open
				resourceType="file"
				displayName="Files"
				onClose={onClose}
			/>,
			{ wrapper: wrapper(deps) }
		);

		fireEvent.change(screen.getByPlaceholderText(/search files/i), {
			target: { value: "new" },
		});

		await waitFor(() => {
			expect(screen.getByText("New File")).toBeInTheDocument();
		});

		const checkbox = screen.getByRole("checkbox", { name: /select new file/i });
		fireEvent.click(checkbox);
		fireEvent.click(screen.getByRole("button", { name: /add 1 selected/i }));

		expect(addCandidateToType).toHaveBeenCalledWith("file", searchResult);
		expect(onClose).toHaveBeenCalled();
	});

	it("disables already-added rows", async () => {
		deps.searchResources = vi.fn().mockResolvedValue([
			{
				externalId: "existing",
				space: "s",
				name: "Existing",
				properties: {},
			},
		]);

		render(
			<AddResourceModal
				open
				resourceType="file"
				displayName="Files"
				onClose={vi.fn()}
			/>,
			{ wrapper: wrapper(deps) }
		);

		fireEvent.change(screen.getByPlaceholderText(/search files/i), {
			target: { value: "ex" },
		});

		await waitFor(() => {
			expect(screen.getByText("Existing")).toBeInTheDocument();
		});

		expect(
			screen.getByRole("checkbox", { name: /select existing/i })
		).toHaveAttribute("aria-disabled", "true");
	});

	it("shows empty results and allows deselecting before add", async () => {
		deps.searchResources = vi.fn().mockResolvedValue([]);

		render(
			<AddResourceModal
				open
				resourceType="file"
				displayName="Files"
				onClose={vi.fn()}
			/>,
			{ wrapper: wrapper(deps) }
		);

		fireEvent.change(screen.getByPlaceholderText(/search files/i), {
			target: { value: "none" },
		});

		await waitFor(() => {
			expect(screen.getByText(/no results for/i)).toBeInTheDocument();
		});
	});

	it("shows search error state", async () => {
		deps.searchResources = vi.fn().mockRejectedValue(new Error("boom"));

		render(
			<AddResourceModal
				open
				resourceType="file"
				displayName="Files"
				onClose={vi.fn()}
			/>,
			{ wrapper: wrapper(deps) }
		);

		fireEvent.change(screen.getByPlaceholderText(/search files/i), {
			target: { value: "err" },
		});

		await waitFor(() => {
			expect(screen.getByText(/search failed/i)).toBeInTheDocument();
		});
	});

	it("toggles selection off and uses properties.name for display", async () => {
		deps.searchResources = vi.fn().mockResolvedValue([
			{
				externalId: "prop-file",
				space: "s",
				properties: { name: "Property Name" },
			},
		]);

		render(
			<AddResourceModal
				open
				resourceType="file"
				displayName="Files"
				onClose={vi.fn()}
			/>,
			{ wrapper: wrapper(deps) }
		);

		fireEvent.change(screen.getByPlaceholderText(/search files/i), {
			target: { value: "prop" },
		});

		await waitFor(() => {
			expect(screen.getByText("Property Name")).toBeInTheDocument();
		});

		const checkbox = screen.getByRole("checkbox", {
			name: /select prop-file/i,
		});
		fireEvent.click(checkbox);
		expect(
			screen.getByRole("button", { name: /add 1 selected/i })
		).toBeEnabled();
		fireEvent.click(checkbox);
		expect(
			screen.getByRole("button", { name: /add\s+selected/i })
		).toBeDisabled();
	});

	it("keeps selections from prior searches when adding", async () => {
		const firstResult: ResourceInstance = {
			externalId: "file-a",
			space: "s",
			name: "File A",
			properties: {},
		};
		const secondResult: ResourceInstance = {
			externalId: "file-b",
			space: "s",
			name: "File B",
			properties: {},
		};
		deps.searchResources = vi
			.fn()
			.mockResolvedValueOnce([firstResult])
			.mockResolvedValueOnce([secondResult]);

		const onClose = vi.fn();
		render(
			<AddResourceModal
				open
				resourceType="file"
				displayName="Files"
				onClose={onClose}
			/>,
			{ wrapper: wrapper(deps) }
		);

		fireEvent.change(screen.getByPlaceholderText(/search files/i), {
			target: { value: "alpha" },
		});
		await waitFor(() => {
			expect(screen.getByText("File A")).toBeInTheDocument();
		});
		fireEvent.click(screen.getByRole("checkbox", { name: /select file a/i }));

		fireEvent.change(screen.getByPlaceholderText(/search files/i), {
			target: { value: "beta" },
		});
		await waitFor(() => {
			expect(screen.getByText("File B")).toBeInTheDocument();
		});
		fireEvent.click(screen.getByRole("checkbox", { name: /select file b/i }));

		expect(
			screen.getByRole("button", { name: /add 2 selected/i })
		).toBeEnabled();
		fireEvent.click(screen.getByRole("button", { name: /add 2 selected/i }));

		expect(addCandidateToType).toHaveBeenCalledWith("file", firstResult);
		expect(addCandidateToType).toHaveBeenCalledWith("file", secondResult);
		expect(addCandidateToType).toHaveBeenCalledTimes(2);
		expect(onClose).toHaveBeenCalled();
	});
});
