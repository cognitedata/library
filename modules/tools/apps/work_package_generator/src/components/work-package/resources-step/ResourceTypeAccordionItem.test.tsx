import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { Accordion } from "@/components/ui";
import { ResourceTypeAccordionItem } from "./ResourceTypeAccordionItem";

describe("ResourceTypeAccordionItem", () => {
	it("shows the type count and adds a resource", async () => {
		const user = userEvent.setup();
		const onAdd = vi.fn();
		render(
			<Accordion type="single" collapsible>
				<ResourceTypeAccordionItem
					typeKey="file"
					config={{
						name: "file",
						displayName: "Files",
						viewId: {
							type: "view",
							space: "cdf_cdm",
							externalId: "CogniteFile",
							version: "v1",
						},
						properties: ["name"],
						resourceName: "file",
						trackingKey: "files",
						assetRelationProperty: "assets",
						assetRelationIsArray: true,
					}}
					items={[]}
					extraProperties={[]}
					onAdd={onAdd}
				/>
			</Accordion>
		);
		expect(screen.getByText("Files")).toBeInTheDocument();
		expect(screen.getByText("0")).toBeInTheDocument();
		await user.click(screen.getByRole("button", { name: /add/i }));
		expect(onAdd).toHaveBeenCalled();
	});
});
