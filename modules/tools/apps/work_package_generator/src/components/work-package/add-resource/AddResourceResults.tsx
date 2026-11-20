import {
	CheckboxItemControl,
	Loader,
	Table,
	TableBody,
	TableCell,
	TableHead,
	TableHeader,
	TableRow,
} from "@/components/ui";
import type { ResourceInstance } from "@/types";

export function AddResourceResults({
	displayName,
	debouncedQuery,
	isFetching,
	error,
	results,
	alreadyAdded,
	selectedItems,
	onToggle,
}: {
	displayName: string;
	debouncedQuery: string;
	isFetching: boolean;
	error: unknown;
	results: ResourceInstance[];
	alreadyAdded: { has: (id: string) => boolean };
	selectedItems: { has: (id: string) => boolean };
	onToggle: (item: ResourceInstance, checked: boolean) => void;
}) {
	return (
		<div className="max-h-64 overflow-y-auto border rounded-md">
			{!debouncedQuery ? (
				<p className="p-4 text-xs text-muted-foreground text-center">
					Type to search for {displayName.toLowerCase()} to add.
				</p>
			) : isFetching ? (
				<div className="flex items-center justify-center gap-2 py-8 text-xs text-muted-foreground">
					<Loader size={16} />
					Searching…
				</div>
			) : error ? (
				<p className="p-4 text-xs text-destructive text-center">
					Search failed. Try again.
				</p>
			) : results.length === 0 ? (
				<p className="p-4 text-xs text-muted-foreground text-center">
					No results for &ldquo;{debouncedQuery}&rdquo;.
				</p>
			) : (
				<Table>
					<TableHeader>
						<TableRow className="bg-muted/30">
							<TableHead className="w-8" />
							<TableHead className="text-xs">Name</TableHead>
							<TableHead className="text-xs">External ID</TableHead>
						</TableRow>
					</TableHeader>
					<TableBody>
						{results.map((item) => {
							const isAdded = alreadyAdded.has(item.externalId);
							const isChecked = isAdded || selectedItems.has(item.externalId);
							const label = item.name || item.externalId;
							return (
								<TableRow
									key={item.externalId}
									data-state={isChecked ? "selected" : undefined}
									className={isAdded ? undefined : "cursor-pointer"}
									onClick={() => {
										if (!isAdded) onToggle(item, !isChecked);
									}}
								>
									<TableCell className="py-1.5">
										<CheckboxItemControl
											checked={isChecked}
											disabled={isAdded}
											aria-label={`Select ${label}`}
											onClick={(event) => event.stopPropagation()}
											onCheckedChange={(checked) => onToggle(item, checked)}
										/>
									</TableCell>
									<TableCell className="text-xs py-1.5 max-w-[160px] truncate">
										{item.name ||
											(typeof item.properties?.name === "string"
												? item.properties.name
												: item.externalId)}
									</TableCell>
									<TableCell className="text-[10px] font-mono py-1.5 text-muted-foreground max-w-[140px] truncate">
										{item.externalId}
									</TableCell>
								</TableRow>
							);
						})}
					</TableBody>
				</Table>
			)}
		</div>
	);
}
