import { fireEvent, render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import type { ResourceInstance } from "@/types";
import {
	PickResourceModal,
	type PickResourceModalDeps,
	PickResourceModalProvider,
} from "./PickResourceModal";

const candidates: ResourceInstance[] = [
	{ externalId: "f1", space: "s", name: "File One", properties: {} },
	{ externalId: "f2", space: "s", name: "File Two", properties: {} },
];

describe("PickResourceModal", () => {
	let setSlotSelection: ReturnType<typeof vi.fn>;
	let deps: PickResourceModalDeps;

	beforeEach(() => {
		setSlotSelection = vi.fn();
		deps = {
			useStore: ((selector: (state: unknown) => unknown) =>
				selector({ setSlotSelection })) as PickResourceModalDeps["useStore"],
		};
	});

	it("selects a resource and closes", () => {
		const onClose = vi.fn();
		render(
			<PickResourceModalProvider value={deps}>
				<PickResourceModal
					open
					slotLabel="My slot"
					candidates={candidates}
					currentSelection={null}
					onClose={onClose}
				/>
			</PickResourceModalProvider>
		);

		fireEvent.click(screen.getAllByRole("button", { name: /^Select$/i })[0]);
		expect(setSlotSelection).toHaveBeenCalledWith("My slot", {
			externalId: "f1",
			space: "s",
			name: "File One",
			description: undefined,
		});
		expect(onClose).toHaveBeenCalled();
	});

	it("clears selection", () => {
		const onClose = vi.fn();
		render(
			<PickResourceModalProvider value={deps}>
				<PickResourceModal
					open
					slotLabel="My slot"
					candidates={candidates}
					currentSelection={{ externalId: "f1", space: "s", name: "File One" }}
					onClose={onClose}
				/>
			</PickResourceModalProvider>
		);

		fireEvent.click(screen.getByRole("button", { name: /clear selection/i }));
		expect(setSlotSelection).toHaveBeenCalledWith("My slot", null);
		expect(onClose).toHaveBeenCalled();
	});

	it("filters candidates by name and shows empty state when none match", () => {
		render(
			<PickResourceModalProvider value={deps}>
				<PickResourceModal
					open
					slotLabel="My slot"
					candidates={candidates}
					currentSelection={null}
					onClose={vi.fn()}
				/>
			</PickResourceModalProvider>
		);

		fireEvent.change(
			screen.getByPlaceholderText(/filter by name, id, or description/i),
			{ target: { value: "Two" } }
		);
		expect(screen.getByText("File Two")).toBeInTheDocument();
		expect(screen.queryByText("File One")).not.toBeInTheDocument();

		fireEvent.change(
			screen.getByPlaceholderText(/filter by name, id, or description/i),
			{ target: { value: "zzz-no-match" } }
		);
		expect(screen.getByText(/no matching resources/i)).toBeInTheDocument();
	});

	it("selects using properties name and description fallbacks", () => {
		const onClose = vi.fn();
		const propertyCandidates: ResourceInstance[] = [
			{
				externalId: "f-props",
				space: "s",
				properties: { name: "Props Name", description: "Props Desc" },
			},
		];

		render(
			<PickResourceModalProvider value={deps}>
				<PickResourceModal
					open
					slotLabel="Props slot"
					candidates={propertyCandidates}
					currentSelection={null}
					onClose={onClose}
				/>
			</PickResourceModalProvider>
		);

		expect(screen.getByText("Props Name")).toBeInTheDocument();
		fireEvent.click(screen.getByRole("button", { name: /^Select$/i }));
		expect(setSlotSelection).toHaveBeenCalledWith("Props slot", {
			externalId: "f-props",
			space: "s",
			name: "Props Name",
			description: "Props Desc",
		});
		expect(onClose).toHaveBeenCalled();
	});
});
