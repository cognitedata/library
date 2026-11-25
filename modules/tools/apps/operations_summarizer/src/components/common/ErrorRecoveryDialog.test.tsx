import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { getDisplayName } from "@/config/dataTypes";
import { ErrorRecoveryDialog } from "./ErrorRecoveryDialog";

const base = {
	open: true,
	onOpenChange: vi.fn(),
	error: "The function timed out",
	canRetry: true,
	onRetry: vi.fn().mockResolvedValue(undefined),
	onResetToIdle: vi.fn(),
	onClose: vi.fn(),
};

describe("ErrorRecoveryDialog", () => {
	it("retries, resets, and closes while showing preserved settings", async () => {
		const onRetry = vi.fn().mockResolvedValue(undefined);
		const onResetToIdle = vi.fn();
		const onClose = vi.fn();
		const onOpenChange = vi.fn();
		render(
			<ErrorRecoveryDialog
				{...base}
				onRetry={onRetry}
				onResetToIdle={onResetToIdle}
				onClose={onClose}
				onOpenChange={onOpenChange}
				operationType="summary generation"
				preservedConfig={{
					dateRange: "Jan 1 – Jan 7",
					dataTypes: ["assets", "files"],
					agent: "Helper",
				}}
			/>
		);

		expect(screen.getByText(/Summary generation Failed/)).toBeInTheDocument();
		expect(screen.getByText(/Date range: Jan 1/)).toBeInTheDocument();
		expect(screen.getByText(/assets, files/)).toBeInTheDocument();
		expect(screen.getByText(/Agent: Helper/)).toBeInTheDocument();

		fireEvent.click(screen.getByRole("button", { name: /^retry$/i }));
		await vi.waitFor(() => expect(onRetry).toHaveBeenCalled());
		expect(onClose).toHaveBeenCalled();

		fireEvent.click(screen.getByRole("button", { name: /^reset$/i }));
		expect(onResetToIdle).toHaveBeenCalled();

		fireEvent.click(
			screen.getByRole("button", { name: /cancel and continue/i })
		);
		expect(onOpenChange).toHaveBeenCalledWith(false);
	});

	it("lists the data types that failed and says nothing was saved", () => {
		render(
			<ErrorRecoveryDialog {...base} failedTypes={["files", "work_orders"]} />
		);
		expect(screen.getByText(/Failed data types:/)).toHaveTextContent(
			`Failed data types: ${getDisplayName("files")}, ${getDisplayName("work_orders")}`
		);
		expect(screen.queryByText(/failed step/i)).not.toBeInTheDocument();
	});

	it("omits the failed types line when there are none", () => {
		render(<ErrorRecoveryDialog {...base} failedTypes={[]} />);
		expect(screen.queryByText(/Failed data types:/)).not.toBeInTheDocument();
	});

	it("keeps the dialog open when retry fails and hides retry when it cannot", async () => {
		const onRetry = vi.fn().mockRejectedValue(new Error("still broken"));
		const onClose = vi.fn();
		const { rerender } = render(
			<ErrorRecoveryDialog {...base} onRetry={onRetry} onClose={onClose} />
		);
		fireEvent.click(screen.getByRole("button", { name: /^retry$/i }));
		await vi.waitFor(() => expect(onRetry).toHaveBeenCalled());
		expect(onClose).not.toHaveBeenCalled();

		rerender(
			<ErrorRecoveryDialog {...base} canRetry={false} onClose={onClose} />
		);
		expect(
			screen.queryByRole("button", { name: /^retry$/i })
		).not.toBeInTheDocument();
	});
});
