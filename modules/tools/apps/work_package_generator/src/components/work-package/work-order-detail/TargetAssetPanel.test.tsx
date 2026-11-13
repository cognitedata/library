import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { TargetAssetPanel } from "./TargetAssetPanel";

describe("TargetAssetPanel", () => {
	it("shows a resolved asset name", () => {
		render(
			<TargetAssetPanel
				asset={{ assetExternalId: "a1", space: "s", assetName: "Pump A" }}
				assetErrorMessage={null}
			/>
		);
		expect(screen.getByText("Pump A")).toBeInTheDocument();
	});

	it("retries a failed resolution", async () => {
		const user = userEvent.setup();
		const onRetry = vi.fn();
		render(
			<TargetAssetPanel
				asset={null}
				assetErrorMessage="Asset lookup failed"
				onRetryAssetResolution={onRetry}
			/>
		);
		await user.click(screen.getByRole("button", { name: /retry asset/i }));
		expect(onRetry).toHaveBeenCalled();
	});
});
