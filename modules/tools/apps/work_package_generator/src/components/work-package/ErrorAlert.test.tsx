import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { ErrorAlert } from "./ErrorAlert";

describe("ErrorAlert", () => {
	it("shows safe message and dismisses", () => {
		const onDismiss = vi.fn();
		render(<ErrorAlert message="Simple error" onDismiss={onDismiss} />);
		expect(screen.getByRole("alert")).toHaveTextContent("Simple error");
		fireEvent.click(screen.getByRole("button", { name: "Dismiss error" }));
		expect(onDismiss).toHaveBeenCalled();
	});
});
