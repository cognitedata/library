import { Check, Search } from "lucide-react";
import { useMemo } from "react";
import { Button, Input } from "@/components/ui";
import type { AiSelections, ResourceInstance } from "@/types";

function matchesFilter(item: ResourceInstance, filter: string): boolean {
	const q = filter.trim().toLowerCase();
	if (!q) return true;
	const name =
		item.name ||
		(typeof item.properties?.name === "string" ? item.properties.name : "");
	const description =
		item.description ||
		(typeof item.properties?.description === "string"
			? item.properties.description
			: "");
	return (
		name.toLowerCase().includes(q) ||
		item.externalId.toLowerCase().includes(q) ||
		description.toLowerCase().includes(q)
	);
}

export function PickResourceList({
	candidates,
	filterText,
	onFilterTextChange,
	currentSelection,
	onSelect,
}: {
	candidates: ResourceInstance[];
	filterText: string;
	onFilterTextChange: (value: string) => void;
	currentSelection: AiSelections[string];
	onSelect: (item: ResourceInstance) => void;
}) {
	const filtered = useMemo(
		() => candidates.filter((c) => matchesFilter(c, filterText)),
		[candidates, filterText]
	);

	return (
		<>
			<div className="relative mb-3">
				<Search
					className="absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-muted-foreground"
					aria-hidden
				/>
				<Input
					value={filterText}
					onChange={(e) => onFilterTextChange(e.target.value)}
					aria-label="Filter resources"
					placeholder="Filter by name, ID, or description…"
					className="pl-8 text-xs"
				/>
			</div>

			<ul className="max-h-64 overflow-y-auto border rounded-md divide-y">
				{filtered.length === 0 ? (
					<li className="p-4 text-xs text-muted-foreground text-center">
						No matching resources.
					</li>
				) : (
					filtered.map((item) => {
						const name =
							item.name ||
							(typeof item.properties?.name === "string"
								? item.properties.name
								: item.externalId);
						const isCurrent =
							currentSelection?.externalId === item.externalId &&
							currentSelection?.space === item.space;

						return (
							<li
								key={item.externalId}
								className={`flex items-center justify-between gap-2 px-3 py-2 text-xs ${
									isCurrent ? "bg-primary/10" : "hover:bg-muted/40"
								}`}
							>
								<div className="min-w-0 flex-1">
									<p className="font-medium truncate">{name}</p>
									<p className="font-mono text-[10px] text-muted-foreground truncate">
										{item.externalId}
									</p>
								</div>
								<Button
									type="button"
									size="sm"
									variant={isCurrent ? "default" : "outline"}
									className="text-xs shrink-0"
									onClick={() => onSelect(item)}
								>
									{isCurrent && <Check className="h-3 w-3" />}
									Select
								</Button>
							</li>
						);
					})
				)}
			</ul>
		</>
	);
}
