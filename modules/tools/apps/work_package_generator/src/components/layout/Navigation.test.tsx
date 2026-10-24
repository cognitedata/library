import { fireEvent, render, screen } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { NavigationProvider } from "@/contexts/NavigationContext";
import { Navigation } from "./Navigation";

describe("Navigation", () => {
	beforeEach(() => {
		document.documentElement.classList.remove("dark");
	});

	afterEach(() => {
		document.documentElement.classList.remove("dark");
	});

	it("toggles dark class on theme control", () => {
		render(
			<NavigationProvider>
				<Navigation />
			</NavigationProvider>
		);
		fireEvent.click(
			screen.getByRole("button", { name: /Switch to dark mode/i })
		);
		expect(document.documentElement.classList.contains("dark")).toBe(true);
		fireEvent.click(
			screen.getByRole("button", { name: /Switch to light mode/i })
		);
		expect(document.documentElement.classList.contains("dark")).toBe(false);
	});

	it("primary nav item is the current page control", () => {
		render(
			<NavigationProvider>
				<Navigation />
			</NavigationProvider>
		);
		const navBtn = screen.getByRole("button", {
			name: "Work package generator",
			current: "page",
		});
		fireEvent.click(navBtn);
		fireEvent.click(
			screen.getByRole("button", {
				name: /Cognite, go to work package generator/i,
			})
		);
	});

	it("opens template canvas instructions from the header info button", () => {
		render(
			<NavigationProvider>
				<Navigation />
			</NavigationProvider>
		);

		fireEvent.click(
			screen.getByRole("button", {
				name: /how to create a work package template/i,
			})
		);

		expect(
			screen.getByRole("dialog", { name: /creating a template canvas/i })
		).toBeInTheDocument();
	});
});
