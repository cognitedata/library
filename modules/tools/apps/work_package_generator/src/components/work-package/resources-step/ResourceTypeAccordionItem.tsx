import { Plus } from "lucide-react";
import {
	AccordionContent,
	AccordionItem,
	AccordionTrigger,
	Badge,
	Button,
} from "@/components/ui";
import type { ResourceConfig, ResourceInstance } from "@/types";
import { ResourceItemsTable } from "./ResourceItemsTable";
import { ResourceTypeIcon } from "./ResourceTypeIcon";

interface ResourceTypeAccordionItemProps {
	typeKey: string;
	config: ResourceConfig;
	items: ResourceInstance[];
	extraProperties: string[];
	onAdd: () => void;
	onRemoveResource?: (typeKey: string, externalId: string) => void;
}

export function ResourceTypeAccordionItem({
	typeKey,
	config,
	items,
	extraProperties,
	onAdd,
	onRemoveResource,
}: ResourceTypeAccordionItemProps) {
	return (
		<AccordionItem value={typeKey} className="border-b-0">
			<div className="flex items-center gap-2 [&>h3]:min-w-0 [&>h3]:flex-1">
				<AccordionTrigger className="py-2 hover:no-underline text-xs">
					<span className="flex items-center gap-2 flex-1">
						<ResourceTypeIcon typeKey={typeKey} />
						<span className="font-medium">{config.displayName}</span>
						<Badge
							variant={items.length > 0 ? "secondary" : "gray"}
							outline={items.length === 0}
							className="text-[10px] px-1.5 py-0"
						>
							{items.length}
						</Badge>
					</span>
				</AccordionTrigger>
				<Button
					type="button"
					variant="outline"
					size="sm"
					className="text-[10px] h-7 shrink-0"
					onClick={onAdd}
				>
					<Plus className="h-3 w-3" />
					Add
				</Button>
			</div>
			<AccordionContent>
				<ResourceItemsTable
					items={items}
					extraProperties={extraProperties}
					displayName={config.displayName}
					onRemoveResource={
						onRemoveResource
							? (externalId) => onRemoveResource(typeKey, externalId)
							: undefined
					}
				/>
			</AccordionContent>
		</AccordionItem>
	);
}
