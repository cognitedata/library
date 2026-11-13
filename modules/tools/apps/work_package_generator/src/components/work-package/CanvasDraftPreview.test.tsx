import { render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import {
	cdfRuntimeQueryWrapper,
	createStubCdfClient,
} from "@/__mocks__/cdf-runtime-test-wrapper";
import { CanvasDraftPreview } from "./CanvasDraftPreview";

// vi.mock: canvas-preview-lib is an external package with no DI; stub avoids loading the real preview runtime.
vi.mock("@dune-canvas/canvas-preview-lib", () => ({
	CanvasPreviewProvider: ({ children }: { children: ReactNode }) => (
		<div data-testid="preview-provider">{children}</div>
	),
	PreviewCanvas: () => <div data-testid="preview-canvas" />,
}));

const wrapper = cdfRuntimeQueryWrapper({
	client: createStubCdfClient(),
	project: "p",
	cluster: "x",
});

describe("CanvasDraftPreview", () => {
	it("shows loading state", () => {
		render(<CanvasDraftPreview nodes={[]} isLoading error={null} />, {
			wrapper,
		});
		expect(screen.getByText(/Building preview/i)).toBeInTheDocument();
	});

	it("shows error message", () => {
		render(
			<CanvasDraftPreview
				nodes={[]}
				isLoading={false}
				error={new Error("preview failed")}
			/>,
			{ wrapper }
		);
		expect(screen.getByText("preview failed")).toBeInTheDocument();
	});

	it("shows empty hint without nodes", () => {
		render(<CanvasDraftPreview nodes={[]} isLoading={false} error={null} />, {
			wrapper,
		});
		expect(screen.getByText(/Nothing to preview yet/i)).toBeInTheDocument();
	});

	it("renders preview canvas when nodes exist", () => {
		render(
			<CanvasDraftPreview
				nodes={[{ id: "n1" } as never]}
				isLoading={false}
				error={null}
			/>,
			{ wrapper }
		);
		expect(screen.getByTestId("preview-canvas")).toBeInTheDocument();
	});
});
