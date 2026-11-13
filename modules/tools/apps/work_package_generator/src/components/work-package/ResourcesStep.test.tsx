import {
	fireEvent,
	render as renderWithoutConfig,
	screen,
} from "@testing-library/react";
import type { ReactElement } from "react";
import { describe, expect, it, vi } from "vitest";
import {
	getResourceConfigs,
	RESOURCES_DISPLAY_LIMIT,
} from "@/__mocks__/app-config";
import { AppConfigProvider } from "@/app-config";
import type { ResourceInstance } from "@/types";
import { ResourcesStep } from "./ResourcesStep";

function render(ui: ReactElement) {
	return renderWithoutConfig(ui, { wrapper: AppConfigProvider });
}

// vi.mock: AddResourceModal pulls CDF/store providers; stub isolates ResourcesStep local modal state.
vi.mock("./AddResourceModal", () => ({
	AddResourceModal: ({
		resourceType,
		displayName,
		onClose,
	}: {
		resourceType: string;
		displayName: string;
		onClose: () => void;
	}) => (
		<div data-testid="add-resource-modal">
			<span>
				{displayName}:{resourceType}
			</span>
			<button type="button" onClick={onClose}>
				Close modal
			</button>
		</div>
	),
}));

const resourceConfigs = getResourceConfigs();

const fileItem: ResourceInstance = {
	externalId: "f1",
	space: "s",
	name: "File one",
	description: "desc",
	properties: { name: "File one", description: "desc" },
};

function makeItems(count: number): ResourceInstance[] {
	return Array.from({ length: count }, (_, i) => ({
		externalId: `item-${i}`,
		space: "s",
		name: `Item ${i}`,
		properties: { name: `Item ${i}` },
	}));
}

describe("ResourcesStep", () => {
	it("shows loading state", () => {
		render(
			<ResourcesStep
				resources={{}}
				resourceConfigs={resourceConfigs}
				isLoadingResources
			/>
		);
		expect(screen.getByText(/Loading resources/i)).toBeInTheDocument();
	});

	it("shows error with retry when handler provided", () => {
		const onRetry = vi.fn();
		render(
			<ResourcesStep
				resources={{}}
				resourceConfigs={resourceConfigs}
				resourcesErrorMessage="Failed to load resources"
				onRetryResources={onRetry}
			/>
		);
		expect(screen.getByRole("alert")).toHaveTextContent(
			"Failed to load resources"
		);
		fireEvent.click(screen.getByRole("button", { name: /Retry/i }));
		expect(onRetry).toHaveBeenCalledTimes(1);
	});

	it("shows error without retry when handler omitted", () => {
		render(
			<ResourcesStep
				resources={{}}
				resourceConfigs={resourceConfigs}
				resourcesErrorMessage="boom"
			/>
		);
		expect(screen.getByRole("alert")).toHaveTextContent("boom");
		expect(
			screen.queryByRole("button", { name: /Retry/i })
		).not.toBeInTheDocument();
	});

	it("summarizes total count with singular wording for one resource", () => {
		render(
			<ResourcesStep
				resources={{ file: [fileItem] }}
				resourceConfigs={{ file: resourceConfigs.file }}
			/>
		);
		expect(
			screen.getByText(/1 total resource found for this work order/i)
		).toBeInTheDocument();
	});

	it("summarizes total count with plural wording", () => {
		render(
			<ResourcesStep
				resources={{ file: [fileItem, { ...fileItem, externalId: "f2" }] }}
				resourceConfigs={{ file: resourceConfigs.file }}
			/>
		);
		expect(
			screen.getByText(/2 total resources found for this work order/i)
		).toBeInTheDocument();
	});

	it("renders resource rows and empty-type message", () => {
		render(
			<ResourcesStep
				resources={{ file: [fileItem], timeseries: [] }}
				resourceConfigs={{
					file: resourceConfigs.file,
					timeseries: resourceConfigs.timeseries,
				}}
			/>
		);
		expect(screen.getByText("File one")).toBeInTheDocument();
		expect(screen.getByText("f1")).toBeInTheDocument();
		expect(
			screen.getByText(/No time series found for this work order/i)
		).toBeInTheDocument();
	});

	it("shows truncation notice when items exceed display limit", () => {
		const items = makeItems(RESOURCES_DISPLAY_LIMIT + 3);
		render(
			<ResourcesStep
				resources={{ file: items }}
				resourceConfigs={{ file: resourceConfigs.file }}
			/>
		);
		expect(
			screen.getByText(
				`Showing ${RESOURCES_DISPLAY_LIMIT} of ${items.length} items`
			)
		).toBeInTheDocument();
		expect(screen.getByText("item-0")).toBeInTheDocument();
		expect(
			screen.queryByText(`item-${RESOURCES_DISPLAY_LIMIT}`)
		).not.toBeInTheDocument();
	});

	it("calls onContinueFromResources when continue is clicked", () => {
		const onContinue = vi.fn();
		render(
			<ResourcesStep
				resources={{ file: [fileItem] }}
				resourceConfigs={{ file: resourceConfigs.file }}
				onContinueFromResources={onContinue}
			/>
		);
		fireEvent.click(
			screen.getByRole("button", { name: /Continue to selections/i })
		);
		expect(onContinue).toHaveBeenCalledTimes(1);
	});

	it("hides continue button when handler is omitted", () => {
		render(
			<ResourcesStep
				resources={{ file: [fileItem] }}
				resourceConfigs={{ file: resourceConfigs.file }}
			/>
		);
		expect(
			screen.queryByRole("button", { name: /Continue to selections/i })
		).not.toBeInTheDocument();
	});

	it("keeps Add as a sibling of the accordion trigger, not a nested button", () => {
		render(
			<ResourcesStep
				resources={{ file: [fileItem] }}
				resourceConfigs={{ file: resourceConfigs.file }}
			/>
		);
		const addButton = screen.getByRole("button", { name: /^Add$/i });
		expect(addButton.closest("button")).toBe(addButton);
		expect(addButton.closest("[data-slot='accordion-trigger']")).toBeNull();
	});

	it("opens and closes AddResourceModal from Add button", () => {
		render(
			<ResourcesStep
				resources={{ file: [fileItem] }}
				resourceConfigs={{ file: resourceConfigs.file }}
			/>
		);
		expect(screen.queryByTestId("add-resource-modal")).not.toBeInTheDocument();
		fireEvent.click(screen.getByRole("button", { name: /^Add$/i }));
		expect(screen.getByTestId("add-resource-modal")).toHaveTextContent(
			"Files:file"
		);
		fireEvent.click(screen.getByRole("button", { name: /Close modal/i }));
		expect(screen.queryByTestId("add-resource-modal")).not.toBeInTheDocument();
	});

	it("renders one Add button per configured resource type", () => {
		render(<ResourcesStep resources={{}} resourceConfigs={resourceConfigs} />);
		expect(screen.getAllByRole("button", { name: /^Add$/i })).toHaveLength(
			Object.keys(resourceConfigs).length
		);
	});

	it("calls onRemoveResource with type and id when remove is clicked", () => {
		const onRemoveResource = vi.fn();
		render(
			<ResourcesStep
				resources={{ file: [fileItem] }}
				resourceConfigs={{ file: resourceConfigs.file }}
				onRemoveResource={onRemoveResource}
			/>
		);
		fireEvent.click(screen.getByRole("button", { name: /Remove File one/i }));
		expect(onRemoveResource).toHaveBeenCalledTimes(1);
		expect(onRemoveResource).toHaveBeenCalledWith("file", "f1");
	});

	it("renders a remove control for every listed resource", () => {
		const second: ResourceInstance = {
			...fileItem,
			externalId: "f2",
			name: "File two",
			properties: { name: "File two", description: "desc" },
		};
		render(
			<ResourcesStep
				resources={{ file: [fileItem, second] }}
				resourceConfigs={{ file: resourceConfigs.file }}
				onRemoveResource={vi.fn()}
			/>
		);
		expect(
			screen.getByRole("button", { name: /Remove File one/i })
		).toBeInTheDocument();
		expect(
			screen.getByRole("button", { name: /Remove File two/i })
		).toBeInTheDocument();
	});

	it("does not render remove controls when onRemoveResource is omitted", () => {
		render(
			<ResourcesStep
				resources={{ file: [fileItem] }}
				resourceConfigs={{ file: resourceConfigs.file }}
			/>
		);
		expect(
			screen.queryByRole("button", { name: /Remove /i })
		).not.toBeInTheDocument();
	});
});
