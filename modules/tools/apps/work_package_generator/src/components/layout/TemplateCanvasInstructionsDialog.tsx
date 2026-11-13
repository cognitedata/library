/**
 * Info dialog with instructions for authoring Work Package Generator template canvases.
 */

import { Info } from "lucide-react";
import { useState } from "react";
import { useAppConfig } from "@/app-config";
import {
	buttonVariants,
	Dialog,
	DialogContent,
	DialogDescription,
	DialogTitle,
	DialogTrigger,
} from "@/components/ui";
import { cn } from "@/utils";
import { instructionSections } from "./templateCanvasInstructionSections";

export function TemplateCanvasInstructionsDialog() {
	const [open, setOpen] = useState(false);
	const { ui } = useAppConfig();
	const sections = instructionSections(
		ui.templateInstructionExamples,
		ui.terms
	);

	return (
		<Dialog open={open} onOpenChange={setOpen}>
			<DialogTrigger
				type="button"
				className={cn(
					buttonVariants({ variant: "ghost", size: "icon-sm" }),
					"shrink-0 text-muted-foreground hover:text-foreground"
				)}
				aria-label="How to create a work package template"
			>
				<Info className="size-4" aria-hidden />
			</DialogTrigger>
			<DialogContent
				closeButtonLabel="Close dialog"
				className="flex max-h-[min(90dvh,48rem)] max-w-2xl flex-col px-0 py-0"
			>
				<div className="flex shrink-0 items-start justify-between gap-3 border-b p-6 pb-4 pr-14">
					<div className="min-w-0">
						<DialogTitle className="text-base font-semibold">
							Creating a Template Canvas
						</DialogTitle>
						<DialogDescription className="mt-1 text-sm text-muted-foreground">
							Follow these steps to author a canvas the Work Package Generator
							can use as a template.
						</DialogDescription>
					</div>
				</div>

				<div className="min-h-0 flex-1 overflow-y-auto p-6 pt-4">
					<ol className="space-y-6">
						{sections.map((section) => (
							<li key={section.title}>
								<h3 className="text-sm font-semibold text-foreground">
									{section.title}
								</h3>
								{"intro" in section && section.intro && (
									<p className="mt-2 text-sm text-muted-foreground">
										{section.intro}
									</p>
								)}
								{"items" in section && section.items && (
									<ul className="mt-2 list-disc space-y-2 pl-5 text-sm text-muted-foreground">
										{section.items.map((item) => (
											<li key={item}>{item}</li>
										))}
									</ul>
								)}
								{"subsections" in section &&
									section.subsections?.map((subsection) => (
										<div key={subsection.label} className="mt-3">
											<p className="text-sm font-medium text-foreground">
												{subsection.label}:
											</p>
											<p className="mt-1 text-sm text-muted-foreground">
												{subsection.text}
											</p>
											{"bullets" in subsection && subsection.bullets && (
												<ul className="mt-1 list-disc space-y-1 pl-5 text-sm text-muted-foreground">
													{subsection.bullets.map((bullet) => (
														<li key={bullet}>{bullet}</li>
													))}
												</ul>
											)}
										</div>
									))}
							</li>
						))}
					</ol>
				</div>
			</DialogContent>
		</Dialog>
	);
}
