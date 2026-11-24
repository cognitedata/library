import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { TargetAssetPanel } from "./TargetAssetPanel";

describe("TargetAssetPanel", () => {
	it("shows a resolved asset name", () => {
		render(
			<TargetAssetPanel
				asset={{ assetExternalId: "a1", space: "s", assetName: "Pump A" }}
				assetLookupState="none"
				assetErrorMessage={null}
			/>
		);
		expect(screen.getByText("Pump A")).toBeInTheDocument();
	});

	it("C05 shows Change work order and Retry when lookup failed", async () => {
		const user = userEvent.setup();
		const onRetry = vi.fn();
		const onChange = vi.fn();
		render(
			<TargetAssetPanel
				asset={null}
				assetLookupState="failed"
				assetErrorMessage="Couldn't load the linked asset."
				onRetryAssetResolution={onRetry}
				onChangeWorkOrder={onChange}
			/>
		);
		await user.click(screen.getByRole("button", { name: "Retry" }));
		await user.click(screen.getByRole("button", { name: "Change work order" }));
		expect(onRetry).toHaveBeenCalled();
		expect(onChange).toHaveBeenCalled();
	});

	it("C05 shows no linked asset instead of a loading hang", () => {
		render(
			<TargetAssetPanel
				asset={null}
				assetLookupState="none"
				assetErrorMessage={null}
			/>
		);
		expect(screen.getByText("No linked asset.")).toBeInTheDocument();
		expect(screen.queryByText("Resolving asset...")).not.toBeInTheDocument();
	});
});
