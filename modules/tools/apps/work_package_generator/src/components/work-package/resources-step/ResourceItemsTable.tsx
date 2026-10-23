import { Trash2 } from "lucide-react";
import { useAppConfig } from "@/app-config";
import {
	Button,
	Table,
	TableBody,
	TableCell,
	TableHead,
	TableHeader,
	TableRow,
} from "@/components/ui";
import type { ResourceInstance } from "@/types";
import { formatPropertyValue, getColumnTitle } from "@/utils";

export function resourceRowLabel(item: ResourceInstance): string {
	const fromProps = item.properties.name;
	if (typeof fromProps === "string" && fromProps.trim()) return fromProps;
	return item.name || item.externalId;
}

interface ResourceItemsTableProps {
	items: ResourceInstance[];
	extraProperties: string[];
	displayName: string;
	onRemoveResource?: (externalId: string) => void;
}

export function ResourceItemsTable({
	items,
	extraProperties,
	displayName,
	onRemoveResource,
}: ResourceItemsTableProps) {
	const resourcesDisplayLimit = useAppConfig().limits.resourcesDisplayLimit;
	if (items.length === 0) {
		return (
			<div className="flex items-center justify-center py-4 text-xs text-muted-foreground border rounded-md bg-muted/10">
				No {displayName.toLowerCase()} found for this work order
			</div>
		);
	}

	return (
		<div className="border rounded-md overflow-hidden max-h-72 overflow-y-auto">
			<Table>
				<TableHeader>
					<TableRow className="bg-muted/30">
						<TableHead className="text-xs py-1.5 font-semibold">Name</TableHead>
						<TableHead className="table-cell text-xs py-1.5 font-semibold mobile:hidden">
							Description
						</TableHead>
						{extraProperties.map((prop) => (
							<TableHead
								key={prop}
								className="text-xs py-1.5 capitalize font-semibold"
							>
								{getColumnTitle(prop)}
							</TableHead>
						))}
						<TableHead className="text-xs py-1.5 font-semibold">
							External ID
						</TableHead>
						{onRemoveResource && (
							<TableHead className="w-8 p-1.5">
								<span className="sr-only">Remove</span>
							</TableHead>
						)}
					</TableRow>
				</TableHeader>
				<TableBody>
					{items.slice(0, resourcesDisplayLimit).map((item) => (
						<TableRow key={item.externalId} className="hover:bg-muted/20">
							<TableCell className="text-xs py-1.5 font-medium max-w-[180px] truncate">
								{formatPropertyValue(item.properties.name) || item.name || "-"}
							</TableCell>
							<TableCell className="table-cell text-xs py-1.5 text-muted-foreground max-w-[200px] truncate mobile:hidden">
								{formatPropertyValue(item.properties.description) ||
									item.description ||
									"-"}
							</TableCell>
							{extraProperties.map((prop) => (
								<TableCell key={prop} className="text-xs py-1.5">
									{formatPropertyValue(item.properties[prop])}
								</TableCell>
							))}
							<TableCell className="font-mono text-[10px] py-1.5 text-muted-foreground max-w-[160px] truncate">
								{item.externalId}
							</TableCell>
							{onRemoveResource && (
								<TableCell className="py-1.5 w-8 text-right">
									<Button
										type="button"
										variant="ghost"
										size="icon-sm"
										className="text-muted-foreground hover:text-destructive"
										aria-label={`Remove ${resourceRowLabel(item)}`}
										onClick={() => onRemoveResource(item.externalId)}
									>
										<Trash2 className="h-3.5 w-3.5" aria-hidden />
									</Button>
								</TableCell>
							)}
						</TableRow>
					))}
				</TableBody>
			</Table>
			{items.length > resourcesDisplayLimit && (
				<div className="px-3 py-1.5 text-[10px] text-muted-foreground bg-muted/50 text-center border-t">
					Showing {resourcesDisplayLimit} of {items.length} items
				</div>
			)}
		</div>
	);
}
