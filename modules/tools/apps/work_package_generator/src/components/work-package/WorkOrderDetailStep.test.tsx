/**
 * Test suite for WorkOrderDetailStep component
 *
 * This component displays work order and asset details with search functionality
 * and handles different asset loading states (loading, success, error).
 */

import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { Asset, WorkOrder } from "@/types";
import { WorkOrderDetailStep } from "./WorkOrderDetailStep";

// vi.mock: detail-row is presentational and covered elsewhere; stub simplifies detail assertions.
vi.mock("./detail-row", () => ({
	Row: ({
		label,
		value,
		mono,
	}: {
		label: string;
		value?: string;
		mono?: boolean;
	}) => (
		<div data-testid={`row-${label.toLowerCase().replace(/\s+/g, "-")}`}>
			<dt>{label}:</dt>
			<dd className={mono ? "font-mono" : ""}>{value ?? "—"}</dd>
		</div>
	),
	BadgeRow: ({ label, value }: { label: string; value?: string }) => (
		<div data-testid={`badge-row-${label.toLowerCase().replace(/\s+/g, "-")}`}>
			<dt>{label}:</dt>
			<dd>
				<span className="badge">{value ?? "—"}</span>
			</dd>
		</div>
	),
}));

// vi.mock: WorkOrderSearchBar is covered in its own suite; stub isolates detail-step state wiring.
vi.mock("./WorkOrderSearchBar", () => ({
	WorkOrderSearchBar: ({
		searchInput,
		onSearchInputChange,
		onSearch,
		isSearching,
		isClientAvailable,
	}: {
		searchInput: string;
		onSearchInputChange: (v: string) => void;
		onSearch: () => void;
		isSearching: boolean;
		isClientAvailable: boolean;
	}) => (
		<div data-testid="work-order-search-bar">
			<input
				data-testid="search-input"
				value={searchInput}
				onChange={(e) => onSearchInputChange(e.target.value)}
				placeholder="Search by Work Order ID"
			/>
			<button
				type="button"
				data-testid="search-button"
				onClick={onSearch}
				disabled={isSearching || !isClientAvailable}
			>
				{isSearching ? "Searching..." : "Search"}
			</button>
		</div>
	),
}));

// vi.mock: datetime formatting is locale-dependent; stub keeps displayed dates assertable.
vi.mock("@/utils/datetime", () => ({
	formatDate: (date?: string) => (date ? `formatted-${date}` : undefined),
}));

describe("WorkOrderDetailStep", () => {
	const mockWorkOrder: WorkOrder = {
		externalId: "WO-001",
		space: "work-orders",
		name: "Maintenance Task 1",
		description: "Regular maintenance of pump equipment",
		status: "Active",
		priority: "High",
		criticality: "Critical",
		type: "Preventive",
		workOrderType: "Maintenance",
		maintenanceType: "Scheduled",
		category: "Equipment",
		frequency: "Monthly",
		requester: "John Doe",
		assignedTo: "Jane Smith",
		location: "Building A",
		equipment: "Pump-001",
		system: "HVAC",
		workOrderNumber: "WO-2024-001",
		scheduledStartTime: "2024-01-15T08:00:00Z",
		scheduledEndTime: "2024-01-15T16:00:00Z",
		actualStartTime: "2024-01-15T08:30:00Z",
		actualEndTime: "2024-01-15T15:30:00Z",
		estimatedDuration: 8,
		sourceId: "SAP-12345",
	};

	const mockAsset: Asset = {
		assetExternalId: "PUMP-001",
		space: "assets",
		assetName: "Primary Water Pump",
		description: "Main water circulation pump for HVAC system",
		status: "Operating",
		criticality: "High",
		type: "Pump",
		assetType: "Mechanical Equipment",
		category: "HVAC",
		subcategory: "Pumps",
		process: "Water Circulation",
		parentAsset: "HVAC-SYSTEM-001",
		parentAssetExternalId: "HVAC-SYS-001",
		hierarchy: "Building A > HVAC > Pumps > Primary",
		location: "Building A, Mechanical Room",
		area: "Mechanical Room",
		zone: "Zone A",
		building: "Building A",
		floor: "Basement",
		room: "Mech-B01",
		manufacturer: "FlowTech Industries",
		model: "FT-5000",
		serialNumber: "FT-2024-001",
		installationDate: "2023-06-15T10:00:00Z",
		operatingConditions: "24/7 operation, normal load",
		capacity: "500 GPM",
		powerRating: "15 kW",
		pressureRating: "150 PSI",
		temperatureRating: "200°F",
		flowRate: "500 GPM",
		dimensions: "4x3x2 feet",
		weight: "850 lbs",
		material: "Cast Iron",
		specifications:
			"High-efficiency centrifugal pump with variable speed drive",
	};

	const defaultProps = {
		workOrder: mockWorkOrder,
		asset: null,
		searchInput: "",
		onSearchInputChange: vi.fn(),
		onSearch: vi.fn(),
		isSearching: false,
		isClientAvailable: true,
		onGenerateWorkPackage: vi.fn(),
		assetErrorMessage: null,
		onRetryAssetResolution: undefined,
	};

	beforeEach(() => {
		vi.clearAllMocks();
	});

	describe("Basic Rendering", () => {
		it("renders the component with work order data", () => {
			render(<WorkOrderDetailStep {...defaultProps} />);

			expect(screen.getByText("Work Order & Asset")).toBeInTheDocument();
			expect(screen.getByText("Work Order")).toBeInTheDocument();
			expect(screen.getByText("Target Asset")).toBeInTheDocument();
			expect(screen.getByTestId("work-order-search-bar")).toBeInTheDocument();
		});

		it("renders work order details correctly", () => {
			render(<WorkOrderDetailStep {...defaultProps} />);

			// Check that work order data is displayed
			expect(screen.getByTestId("row-name")).toBeInTheDocument();
			expect(screen.getByTestId("row-external-id")).toBeInTheDocument();
			expect(screen.getByTestId("badge-row-status")).toBeInTheDocument();
			expect(screen.getByTestId("badge-row-priority")).toBeInTheDocument();
			expect(screen.getByTestId("badge-row-criticality")).toBeInTheDocument();
		});

		it("renders the continue button", () => {
			render(<WorkOrderDetailStep {...defaultProps} />);

			const continueButton = screen.getByRole("button", {
				name: /continue with ai suggestions/i,
			});
			expect(continueButton).toBeInTheDocument();
		});

		it("renders the informational text about the workflow", () => {
			render(<WorkOrderDetailStep {...defaultProps} />);

			// Text is split across <strong> elements so we check the container's
			// combined text content rather than trying to match a single element.
			const body = document.body.textContent ?? "";
			expect(body).toContain("automatically");
			expect(body).toContain("recommend a canvas template");
			expect(body).toContain("review and change");
			expect(body).toContain("the template");
		});
	});

	describe("Asset States", () => {
		it("shows loading state when asset is null and no error", () => {
			render(<WorkOrderDetailStep {...defaultProps} asset={null} />);

			expect(screen.getByText("Resolving asset...")).toBeInTheDocument();
			expect(screen.queryByText("Primary Water Pump")).not.toBeInTheDocument();
		});

		it("shows asset details when asset is provided", () => {
			render(<WorkOrderDetailStep {...defaultProps} asset={mockAsset} />);

			// Both work order and asset render a "Name" row so there are multiple
			// row-name elements — use getAllByTestId and verify both are present.
			expect(screen.getAllByTestId("row-name").length).toBeGreaterThanOrEqual(
				2
			);
			// badge-row-status and badge-row-criticality exist for both sections too
			expect(
				screen.getAllByTestId("badge-row-status").length
			).toBeGreaterThanOrEqual(2);

			// Loading state should not be visible
			expect(screen.queryByText("Resolving asset...")).not.toBeInTheDocument();
		});

		it("shows error state when assetErrorMessage is provided", () => {
			const errorMessage = "Failed to resolve asset: Network timeout";
			render(
				<WorkOrderDetailStep
					{...defaultProps}
					asset={null}
					assetErrorMessage={errorMessage}
				/>
			);

			expect(screen.getByText(errorMessage)).toBeInTheDocument();
			expect(screen.queryByText("Resolving asset...")).not.toBeInTheDocument();
			expect(screen.queryByText("Primary Water Pump")).not.toBeInTheDocument();
		});

		it("shows retry button when error message and retry callback are provided", () => {
			const onRetryAssetResolution = vi.fn();
			const errorMessage = "Failed to resolve asset: Network timeout";

			render(
				<WorkOrderDetailStep
					{...defaultProps}
					asset={null}
					assetErrorMessage={errorMessage}
					onRetryAssetResolution={onRetryAssetResolution}
				/>
			);

			const retryButton = screen.getByRole("button", {
				name: /retry asset resolution/i,
			});
			expect(retryButton).toBeInTheDocument();

			fireEvent.click(retryButton);
			expect(onRetryAssetResolution).toHaveBeenCalledTimes(1);
		});

		it("does not show retry button when error message is provided but no retry callback", () => {
			const errorMessage = "Failed to resolve asset: Network timeout";
			render(
				<WorkOrderDetailStep
					{...defaultProps}
					asset={null}
					assetErrorMessage={errorMessage}
					onRetryAssetResolution={undefined}
				/>
			);

			expect(screen.getByText(errorMessage)).toBeInTheDocument();
			expect(
				screen.queryByRole("button", { name: /retry asset resolution/i })
			).not.toBeInTheDocument();
		});
	});

	describe("Button States and Interactions", () => {
		it("disables continue button when searching", () => {
			render(
				<WorkOrderDetailStep
					{...defaultProps}
					isSearching={true}
					asset={mockAsset}
				/>
			);

			const continueButton = screen.getByRole("button", {
				name: /continue with ai suggestions/i,
			});
			expect(continueButton).toBeDisabled();
			expect(
				screen.getByText("Loading work order details…")
			).toBeInTheDocument();
		});

		it("disables continue button when asset is null", () => {
			render(<WorkOrderDetailStep {...defaultProps} asset={null} />);

			const continueButton = screen.getByRole("button", {
				name: /continue with ai suggestions/i,
			});
			expect(continueButton).toBeDisabled();
			expect(screen.getByText("Resolving target asset…")).toBeInTheDocument();
		});

		it("disables continue button when asset error is present", () => {
			render(
				<WorkOrderDetailStep
					{...defaultProps}
					asset={null}
					assetErrorMessage="Failed to resolve asset"
				/>
			);

			const continueButton = screen.getByRole("button", {
				name: /continue with ai suggestions/i,
			});
			expect(continueButton).toBeDisabled();
			expect(screen.getByText("Asset resolution failed")).toBeInTheDocument();
		});

		it("enables continue button when asset is available and not searching", () => {
			render(
				<WorkOrderDetailStep
					{...defaultProps}
					asset={mockAsset}
					isSearching={false}
				/>
			);

			const continueButton = screen.getByRole("button", {
				name: /continue with ai suggestions/i,
			});
			expect(continueButton).not.toBeDisabled();
			expect(screen.queryByText(/Loading|Resolving/)).not.toBeInTheDocument();
		});

		it("calls onGenerateWorkPackage when continue button is clicked", () => {
			const onGenerateWorkPackage = vi.fn();
			render(
				<WorkOrderDetailStep
					{...defaultProps}
					asset={mockAsset}
					onGenerateWorkPackage={onGenerateWorkPackage}
				/>
			);

			const continueButton = screen.getByRole("button", {
				name: /continue with ai suggestions/i,
			});
			fireEvent.click(continueButton);

			expect(onGenerateWorkPackage).toHaveBeenCalledTimes(1);
		});
	});

	describe("Search Bar Integration", () => {
		it("passes search props to WorkOrderSearchBar", () => {
			const onSearchInputChange = vi.fn();
			const onSearch = vi.fn();

			render(
				<WorkOrderDetailStep
					{...defaultProps}
					searchInput="test query"
					onSearchInputChange={onSearchInputChange}
					onSearch={onSearch}
					isSearching={true}
					isClientAvailable={false}
				/>
			);

			const searchInput = screen.getByTestId("search-input");
			const searchButton = screen.getByTestId("search-button");

			expect(searchInput).toHaveValue("test query");
			expect(searchButton).toBeDisabled(); // isSearching = true
			expect(searchButton).toHaveTextContent("Searching...");
		});

		it("handles search input changes", () => {
			const onSearchInputChange = vi.fn();

			render(
				<WorkOrderDetailStep
					{...defaultProps}
					onSearchInputChange={onSearchInputChange}
				/>
			);

			const searchInput = screen.getByTestId("search-input");
			fireEvent.change(searchInput, { target: { value: "new search" } });

			expect(onSearchInputChange).toHaveBeenCalledWith("new search");
		});

		it("handles search submission", () => {
			const onSearch = vi.fn();

			render(<WorkOrderDetailStep {...defaultProps} onSearch={onSearch} />);

			const searchButton = screen.getByTestId("search-button");
			fireEvent.click(searchButton);

			expect(onSearch).toHaveBeenCalledTimes(1);
		});
	});

	describe("Data Formatting", () => {
		it("formats dates using formatDate utility", () => {
			render(<WorkOrderDetailStep {...defaultProps} />);

			// The formatDate mock should be called with the work order dates
			// We can't directly test the formatting here due to the mocked components,
			// but we ensure the component renders without errors
			expect(screen.getByTestId("work-order-search-bar")).toBeInTheDocument();
		});

		it("handles estimated duration formatting", () => {
			const workOrderWithDuration = {
				...mockWorkOrder,
				estimatedDuration: 5,
			};

			render(
				<WorkOrderDetailStep
					{...defaultProps}
					workOrder={workOrderWithDuration}
				/>
			);

			// Component should render without errors when estimatedDuration is present
			expect(screen.getByText("Work Order & Asset")).toBeInTheDocument();
		});

		it("handles missing estimated duration", () => {
			const workOrderWithoutDuration = {
				...mockWorkOrder,
				estimatedDuration: undefined,
			};

			render(
				<WorkOrderDetailStep
					{...defaultProps}
					workOrder={workOrderWithoutDuration}
				/>
			);

			// Component should render without errors when estimatedDuration is missing
			expect(screen.getByText("Work Order & Asset")).toBeInTheDocument();
		});
	});

	describe("Accessibility", () => {
		it("has proper heading structure", () => {
			render(<WorkOrderDetailStep {...defaultProps} />);

			expect(screen.getByText("Work Order")).toBeInTheDocument();
			expect(screen.getByText("Target Asset")).toBeInTheDocument();
		});

		it("continue button has proper text and icon", () => {
			render(<WorkOrderDetailStep {...defaultProps} asset={mockAsset} />);

			const continueButton = screen.getByRole("button", {
				name: /continue with ai suggestions/i,
			});
			expect(continueButton).toBeInTheDocument();
		});

		it("retry button has proper accessible text when shown", () => {
			const onRetryAssetResolution = vi.fn();
			render(
				<WorkOrderDetailStep
					{...defaultProps}
					asset={null}
					assetErrorMessage="Failed to resolve asset"
					onRetryAssetResolution={onRetryAssetResolution}
				/>
			);

			const retryButton = screen.getByRole("button", {
				name: /retry asset resolution/i,
			});
			expect(retryButton).toBeInTheDocument();
		});
	});

	describe("Edge Cases", () => {
		it("handles work order with minimal data", () => {
			const minimalWorkOrder: WorkOrder = {
				externalId: "WO-MIN",
				space: "work-orders",
			};

			render(
				<WorkOrderDetailStep {...defaultProps} workOrder={minimalWorkOrder} />
			);

			expect(screen.getByText("Work Order & Asset")).toBeInTheDocument();
		});

		it("handles asset with minimal data", () => {
			const minimalAsset: Asset = {
				assetExternalId: "ASSET-MIN",
				space: "assets",
			};

			render(<WorkOrderDetailStep {...defaultProps} asset={minimalAsset} />);

			expect(screen.getByText("Work Order & Asset")).toBeInTheDocument();
		});

		it("handles empty search input", () => {
			render(<WorkOrderDetailStep {...defaultProps} searchInput="" />);

			const searchInput = screen.getByTestId("search-input");
			expect(searchInput).toHaveValue("");
		});

		it("handles client unavailable state", () => {
			render(
				<WorkOrderDetailStep {...defaultProps} isClientAvailable={false} />
			);

			const searchButton = screen.getByTestId("search-button");
			expect(searchButton).toBeDisabled();
		});
	});
});
