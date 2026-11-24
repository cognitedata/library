import { render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import {
	Accordion,
	AccordionContent,
	AccordionItem,
	AccordionTrigger,
} from "@/components/ui";

describe("Accordion UI", () => {
	it("renders the trigger inside a heading so a sibling action can sit beside it", () => {
		render(
			<Accordion type="single" collapsible>
				<AccordionItem value="a">
					<AccordionTrigger>Title</AccordionTrigger>
					<AccordionContent>Body</AccordionContent>
				</AccordionItem>
			</Accordion>
		);
		expect(screen.getByRole("heading", { name: "Title" })).toBeInTheDocument();
	});
});
