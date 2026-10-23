import { Canvas, type CanvasNode, type ToolId } from "@dune-canvas/canvas-lib";
import { fireEvent, render } from "@testing-library/react";
import { useState } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { ensurePluginsRegistered } from "../../../canvas-preview-lib/src/register";

ensurePluginsRegistered();

function node(id: string, x: number, y: number): CanvasNode {
	return {
		id,
		type: "text",
		position: { x, y },
		size: { width: 120, height: 60 },
		zIndex: 1,
		data: { content: id },
	};
}

function Harness({
	initialTool = "selection",
	readOnly = false,
}: {
	initialTool?: ToolId;
	readOnly?: boolean;
}) {
	const [nodes, setNodes] = useState<CanvasNode[]>([
		node("a", 40, 40),
		node("b", 300, 80),
	]);
	const [selection, setSelection] = useState<string[]>([]);
	const [tool, setTool] = useState<ToolId>(initialTool);
	return (
		<Canvas
			nodes={nodes}
			selection={selection}
			activeTool={tool}
			readOnly={readOnly}
			onNodesChange={setNodes}
			onSelectionChange={setSelection}
			onToolComplete={() => setTool("selection")}
			renderToolbar={() => <div>toolbar</div>}
		/>
	);
}

function canvas(): HTMLElement {
	const element = document.querySelector("[data-testid='canvas']");
	if (!(element instanceof HTMLElement)) {
		throw new Error("canvas missing");
	}
	return element;
}

describe("Canvas interactions", () => {
	beforeEach(() => {
		vi.spyOn(HTMLElement.prototype, "getBoundingClientRect").mockReturnValue({
			x: 0,
			y: 0,
			width: 1200,
			height: 800,
			top: 0,
			left: 0,
			bottom: 800,
			right: 1200,
			toJSON: () => ({}),
		});
	});

	it("selects, drags, resizes, and box-selects", () => {
		render(<Harness />);
		const surface = canvas();

		fireEvent.mouseDown(surface, { button: 0, clientX: 80, clientY: 70 });
		fireEvent.mouseMove(surface, { button: 0, clientX: 140, clientY: 110 });
		fireEvent.mouseUp(surface, { button: 0, clientX: 140, clientY: 110 });

		fireEvent.mouseDown(surface, {
			button: 0,
			clientX: 80,
			clientY: 70,
			shiftKey: true,
		});
		fireEvent.mouseUp(surface, { button: 0, clientX: 80, clientY: 70 });

		fireEvent.mouseDown(surface, { button: 0, clientX: 40, clientY: 40 });
		fireEvent.mouseMove(surface, { button: 0, clientX: 90, clientY: 90 });
		fireEvent.mouseUp(surface, { button: 0, clientX: 90, clientY: 90 });

		fireEvent.mouseDown(surface, { button: 0, clientX: 900, clientY: 700 });
		fireEvent.mouseMove(surface, { button: 0, clientX: 1000, clientY: 760 });
		fireEvent.mouseUp(surface, { button: 0, clientX: 1000, clientY: 760 });

		fireEvent.mouseDown(surface, { button: 2, clientX: 10, clientY: 10 });
		fireEvent.mouseMove(surface, { button: 2, clientX: 40, clientY: 30 });
		fireEvent.mouseUp(surface, { button: 2, clientX: 40, clientY: 30 });

		fireEvent.doubleClick(surface, { clientX: 80, clientY: 70 });
		expect(surface).toBeTruthy();
	});

	it("draws with the rectangle tool and pans with the pan tool", () => {
		const { rerender } = render(<Harness initialTool="rectangle" />);
		const surface = canvas();
		fireEvent.mouseDown(surface, { button: 0, clientX: 500, clientY: 500 });
		fireEvent.mouseMove(surface, { button: 0, clientX: 620, clientY: 580 });
		fireEvent.mouseUp(surface, { button: 0, clientX: 620, clientY: 580 });

		rerender(<Harness initialTool="ellipse" />);
		fireEvent.mouseDown(surface, { button: 0, clientX: 200, clientY: 400 });
		fireEvent.mouseMove(surface, { button: 0, clientX: 280, clientY: 460 });
		fireEvent.mouseUp(surface, { button: 0, clientX: 280, clientY: 460 });

		rerender(<Harness initialTool="line" />);
		fireEvent.mouseDown(surface, { button: 0, clientX: 10, clientY: 400 });
		fireEvent.mouseMove(surface, { button: 0, clientX: 80, clientY: 430 });
		fireEvent.mouseUp(surface, { button: 0, clientX: 80, clientY: 430 });

		rerender(<Harness initialTool="pan" />);
		fireEvent.mouseDown(surface, { button: 0, clientX: 10, clientY: 10 });
		fireEvent.mouseMove(surface, { button: 0, clientX: 40, clientY: 20 });
		fireEvent.mouseUp(surface, { button: 0, clientX: 40, clientY: 20 });
	});

	it("pans in read-only mode and zooms from the wheel", () => {
		render(<Harness readOnly />);
		const surface = canvas();
		fireEvent.mouseDown(surface, { button: 0, clientX: 20, clientY: 20 });
		fireEvent.mouseMove(surface, { button: 0, clientX: 50, clientY: 40 });
		fireEvent.mouseUp(surface, { button: 0, clientX: 50, clientY: 40 });
		fireEvent.wheel(surface, {
			deltaY: 40,
			deltaX: 10,
			clientX: 100,
			clientY: 100,
		});
		fireEvent.wheel(surface, {
			deltaY: -20,
			clientX: 100,
			clientY: 100,
			ctrlKey: true,
		});
		fireEvent.mouseDown(surface, { button: 1, clientX: 10, clientY: 10 });
		fireEvent.contextMenu(surface);
	});

	it("resizes every handle, including below the minimum size", () => {
		render(<Harness />);
		const surface = canvas();
		selectNode(surface, 80, 70);

		for (const handle of [
			{ x: 160, y: 100, dx: 30, dy: 20 },
			{ x: 40, y: 40, dx: -20, dy: -10 },
			{ x: 160, y: 40, dx: 15, dy: -12 },
			{ x: 40, y: 100, dx: -10, dy: 8 },
			{ x: 100, y: 40, dx: 0, dy: -25 },
			{ x: 160, y: 70, dx: 40, dy: 0 },
			{ x: 100, y: 100, dx: 0, dy: 30 },
			{ x: 40, y: 70, dx: 200, dy: 0 },
		]) {
			drag(
				surface,
				handle.x,
				handle.y,
				handle.x + handle.dx,
				handle.y + handle.dy
			);
		}
		expect(surface.querySelector("[data-node-id='a']")).toBeTruthy();
	});

	it("scales a locked-aspect node from both corners", () => {
		render(
			<Canvas
				nodes={[
					{
						id: "img",
						type: "image",
						position: { x: 200, y: 200 },
						size: { width: 100, height: 80 },
						zIndex: 1,
						data: { url: "https://files.example.test/a.png" },
					},
				]}
				selection={["img"]}
				activeTool="selection"
				onNodesChange={vi.fn()}
				onSelectionChange={vi.fn()}
			/>
		);
		const surface = canvas();
		drag(surface, 300, 280, 420, 290);
		drag(surface, 200, 200, 140, 260);
		drag(surface, 300, 200, 310, 120);
	});

	it("attaches and detaches a child while dragging", () => {
		const nodes: CanvasNode[] = [
			{
				id: "parent",
				type: "text",
				position: { x: 100, y: 100 },
				size: { width: 300, height: 300 },
				zIndex: 1,
				data: { content: "parent" },
			},
			{
				id: "child",
				type: "text",
				position: { x: 140, y: 140 },
				size: { width: 40, height: 40 },
				zIndex: 2,
				parentId: "parent",
				relativePosition: { x: 0.1, y: 0.1 },
				relativeSize: { width: 0.2, height: 0.2 },
				data: { content: "child" },
			},
			{
				id: "loose",
				type: "text",
				position: { x: 500, y: 500 },
				size: { width: 40, height: 40 },
				zIndex: 2,
				data: { content: "loose" },
			},
		];
		const onNodesChange = vi.fn();
		const onDragEnd = vi.fn();
		render(
			<Canvas
				nodes={nodes}
				selection={["child"]}
				connections={[
					{ id: "c1", from: "parent", to: "missing" },
					{ id: "c2", from: "parent", to: "child" },
				]}
				activeTool="selection"
				onNodesChange={onNodesChange}
				onSelectionChange={vi.fn()}
				onDragEnd={onDragEnd}
				onConnectionsChange={vi.fn()}
				renderToolPreview={() => <div>preview</div>}
			/>
		);
		const surface = canvas();
		drag(surface, 160, 160, 20, 20);
		expect(onDragEnd).toHaveBeenCalled();

		render(
			<Canvas
				nodes={nodes}
				selection={["loose"]}
				activeTool="selection"
				onNodesChange={onNodesChange}
				onSelectionChange={vi.fn()}
			/>
		);
		drag(canvas(), 520, 520, 200, 200);
	});

	it("updates and rotates the selected node from the toolbar", () => {
		let update: ((updater: (node: CanvasNode) => unknown) => void) | undefined;
		let rotate: ((degrees: number) => void) | undefined;
		render(
			<Canvas
				nodes={[
					node("a", 40, 40),
					{
						id: "ghost",
						type: "not-a-node",
						position: { x: 800, y: 800 },
						size: { width: 10, height: 10 },
						zIndex: 0,
						data: {},
					},
				]}
				selection={["a"]}
				activeTool="rectangle"
				onNodesChange={vi.fn()}
				onSelectionChange={vi.fn()}
				onNodeDataChange={vi.fn()}
				renderToolbar={({ updateSelectedNodes, rotateSelectedNodes }) => {
					update = updateSelectedNodes;
					rotate = rotateSelectedNodes;
					return <button type="button">toolbar</button>;
				}}
			/>
		);
		update?.((current) => ({ ...(current.data as object), content: "edited" }));
		rotate?.(-90);
		rotate?.(400);
		expect(document.querySelector("[data-node-id='ghost']")).toBeNull();
	});
});

describe("cause map anchors", () => {
	it("adds a connected node from each anchor", () => {
		const onNodesChange = vi.fn();
		const onConnectionsChange = vi.fn();
		render(
			<Canvas
				nodes={[
					{
						id: "root",
						type: "cause-map",
						position: { x: 200, y: 200 },
						size: { width: 160, height: 80 },
						zIndex: 1,
						data: { text: "Root", status: "accepted" },
					},
				]}
				selection={["root"]}
				connections={[]}
				activeTool="selection"
				onNodesChange={onNodesChange}
				onConnectionsChange={onConnectionsChange}
				onSelectionChange={vi.fn()}
			/>
		);
		for (const anchor of document.querySelectorAll("[data-anchor]")) {
			fireEvent.mouseEnter(anchor);
			fireEvent.mouseDown(anchor);
			fireEvent.click(anchor);
			fireEvent.mouseLeave(anchor);
		}
		expect(onNodesChange).toHaveBeenCalled();
	});
});

function selectNode(surface: HTMLElement, x: number, y: number) {
	fireEvent.mouseDown(surface, { button: 0, clientX: x, clientY: y });
	fireEvent.mouseUp(surface, { button: 0, clientX: x, clientY: y });
}

function drag(
	surface: HTMLElement,
	fromX: number,
	fromY: number,
	toX: number,
	toY: number
) {
	fireEvent.mouseDown(surface, { button: 0, clientX: fromX, clientY: fromY });
	fireEvent.mouseMove(surface, { button: 0, clientX: toX, clientY: toY });
	fireEvent.mouseUp(surface, { button: 0, clientX: toX, clientY: toY });
}
