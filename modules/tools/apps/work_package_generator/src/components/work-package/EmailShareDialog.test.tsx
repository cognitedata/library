import { fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { EmailShareDialog } from "./EmailShareDialog";

describe("EmailShareDialog", () => {
	beforeEach(() => {
		vi.stubGlobal("open", vi.fn());
	});

	afterEach(() => {
		vi.unstubAllGlobals();
	});

	it("renders the Email link button", () => {
		render(
			<EmailShareDialog canvasLink="https://cdf/c" canvasName="My canvas" />
		);

		expect(
			screen.getByRole("button", { name: /Email link/i })
		).toBeInTheDocument();
	});

	it("opens a mailto link when clicked", () => {
		render(
			<EmailShareDialog canvasLink="https://cdf/c" canvasName="My canvas" />
		);

		fireEvent.click(screen.getByRole("button", { name: /Email link/i }));

		expect(window.open).toHaveBeenCalledTimes(1);
		const [url, target] = (window.open as ReturnType<typeof vi.fn>).mock
			.calls[0];
		expect(url).toMatch(/^mailto:/);
		expect(url).toContain(encodeURIComponent("Canvas: My canvas"));
		expect(url).toContain(encodeURIComponent("https://cdf/c"));
		expect(target).toBe("_blank");
	});

	it("encodes the canvas name and link into the subject and body", () => {
		render(
			<EmailShareDialog
				canvasLink="https://example.com/canvas"
				canvasName="Pump Inspection"
			/>
		);

		fireEvent.click(screen.getByRole("button", { name: /Email link/i }));

		const url = (window.open as ReturnType<typeof vi.fn>).mock
			.calls[0][0] as string;
		expect(url).toContain(encodeURIComponent("Canvas: Pump Inspection"));
		expect(url).toContain(encodeURIComponent("https://example.com/canvas"));
	});
});
