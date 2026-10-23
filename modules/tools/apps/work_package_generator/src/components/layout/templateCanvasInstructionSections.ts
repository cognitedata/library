/**
 * Copy for the template canvas instructions dialog.
 */

export function instructionSections(
	examples: {
		workOrderId: string;
		functionalLocationId: string;
	},
	terms: { workOrder: string; workPackage: string; functionalLocation: string }
) {
	return [
		{
			title: "1. Create and Configure a New Canvas",
			items: [
				"Open Canvas: Navigate to the Industrial Canvas application within your workspace.",
				"Create New: Click to create a fresh, blank canvas.",
				'Name Appropriately: Rename the canvas ensuring it explicitly includes the word "Template" (e.g., Template Steam Valve). This clearly distinguishes it from active, asset-specific canvases.',
				`Tag the Canvas: Add the official "${terms.workPackage}s" tag at the top of the canvas. This metadata tag is critical; it is how the Work Package Generator app identifies and indexes valid layouts.`,
			],
		},
		{
			title: "2. Build the Layout Structure",
			intro:
				"Use the native canvas sidebar tools to establish a clean, scannable structure:",
			items: [
				"Sections & Containers: Utilize the Shape tool (rectangles/borders) and Line tool to visually compartmentalize the page into logical zones (e.g., Header, Safety, Maintenance Operations, Assets).",
				"Labels & Titles: Use the Text tool to add static titles, headers, and instructional labels so field crews can easily navigate the final generated document.",
			],
		},
		{
			title: "3. Embed Example Data (Placeholders)",
			intro:
				"The Work Package Generator relies on sample data nodes as temporary placeholders. When a live package is run, the app dynamically swaps these out for the actual target assets.",
			subsections: [
				{
					label: "Add Objects",
					text: "Drag and drop example data elements onto your canvas. For a standard valve template, you should include:",
					bullets: [
						`${terms.workOrder}s: (e.g., ${examples.workOrderId})`,
						"Equipment/Components: (e.g., Equipment - 11041065)",
						`${terms.functionalLocation}s: (e.g., ${examples.functionalLocationId})`,
					],
				},
				{
					label: "Arrange Logic",
					text: "Place these objects into their respective structural sections built in Step 2.",
				},
				{
					label: "Update Object Labels",
					text: "Ensure each data element features a descriptive label that identifies its specific category (e.g., WorkOrder-1).",
				},
			],
		},
		{
			title: "4. Publish and Verify Integration",
			items: [
				"Publish: Once you are satisfied with the layout and placeholder elements, click the Publish button to lock in the template version.",
				"Verification: Open the Work Package Generator Flows app. Confirm that your newly created template is visible and successfully pulled into the template dropdown selection menu.",
			],
		},
	];
}
