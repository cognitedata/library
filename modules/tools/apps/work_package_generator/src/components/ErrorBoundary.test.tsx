import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ErrorBoundary } from "./ErrorBoundary";

function Boom({ shouldThrow }: { shouldThrow: boolean }) {
	if (shouldThrow) throw new Error("boom");
	return <span>ok</span>;
}

describe("ErrorBoundary", () => {
	it("renders children when healthy", () => {
		render(
			<ErrorBoundary>
				<Boom shouldThrow={false} />
			</ErrorBoundary>
		);
		expect(screen.getByText("ok")).toBeInTheDocument();
	});

	it("shows error message in fallback", () => {
		const errSpy = vi.spyOn(console, "error").mockImplementation(() => {});
		render(
			<ErrorBoundary>
				<Boom shouldThrow />
			</ErrorBoundary>
		);
		expect(screen.getByRole("alert")).toHaveTextContent("Something went wrong");
		expect(screen.getByText("boom")).toBeInTheDocument();
		fireEvent.click(screen.getByRole("button", { name: "Try again" }));
		errSpy.mockRestore();
	});

	it("uses custom fallback", () => {
		const errSpy = vi.spyOn(console, "error").mockImplementation(() => {});
		render(
			<ErrorBoundary fallback={<div>custom</div>}>
				<Boom shouldThrow />
			</ErrorBoundary>
		);
		expect(screen.getByText("custom")).toBeInTheDocument();
		errSpy.mockRestore();
	});
});
