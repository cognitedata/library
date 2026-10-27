import { Activity, ArrowRight } from "lucide-react";
import { useMemo, useState } from "react";
import { Accordion, Badge, Button } from "@/components/ui";
import type { ResourceConfig, ResourceInstance } from "@/types";
import { AddResourceModal } from "./AddResourceModal";
import { ResourcesStepStatus } from "./resources-step/ResourcesStepStatus";
import { ResourceTypeAccordionItem } from "./resources-step/ResourceTypeAccordionItem";
import { ResourceTypeIcon } from "./resources-step/ResourceTypeIcon";

interface ResourcesStepProps {
	resources: Record<string, ResourceInstance[]>;
	resourceConfigs: Record<string, ResourceConfig>;
	isLoadingResources?: boolean;
	resourcesErrorMessage?: string | null;
	onRetryResources?: () => void;
	onContinueFromResources?: () => void;
	onRemoveResource?: (typeKey: string, externalId: string) => void;
}

export function ResourcesStep({
	resources,
	resourceConfigs,
	isLoadingResources = false,
	resourcesErrorMessage = null,
	onRetryResources,
	onContinueFromResources,
	onRemoveResource,
}: ResourcesStepProps) {
	const [openModalType, setOpenModalType] = useState<string | null>(null);

	const totalResources = useMemo(
		() =>
			Object.values(resources).reduce((sum, items) => sum + items.length, 0),
		[resources]
	);

	const extraProperties = useMemo(() => {
		const map: Record<string, string[]> = {};
		for (const [typeKey, config] of Object.entries(resourceConfigs)) {
			map[typeKey] = config.properties.filter(
				(p) => p !== "name" && p !== "description"
			);
		}
		return map;
	}, [resourceConfigs]);

	if (isLoadingResources || resourcesErrorMessage) {
		return (
			<ResourcesStepStatus
				isLoading={isLoadingResources}
				errorMessage={resourcesErrorMessage}
				onRetry={onRetryResources}
			/>
		);
	}

	return (
		<div className="p-3 space-y-3">
			<h3 className="text-sm font-semibold flex items-center gap-2">
				<Activity className="h-4 w-4" />
				Available Resources
			</h3>
			<div className="flex flex-wrap items-center gap-2">
				<span className="text-xs text-muted-foreground">
					{totalResources} total resource
					{totalResources !== 1 ? "s" : ""} found for this work order
				</span>
				<span className="text-muted-foreground/40">|</span>
				{Object.entries(resourceConfigs).map(([typeKey, config]) => {
					const count = (resources[typeKey] || []).length;
					return (
						<Badge
							key={typeKey}
							variant={count > 0 ? "secondary" : "gray"}
							outline={count === 0}
							className="text-[10px]"
						>
							<ResourceTypeIcon typeKey={typeKey} />
							{count} {config.displayName}
						</Badge>
					);
				})}
			</div>
			{onContinueFromResources && (
				<div className="border-border border-t pt-3">
					<Button
						type="button"
						size="lg"
						className="w-full mobile:w-full sm:w-auto"
						onClick={onContinueFromResources}
					>
						<ArrowRight className="size-4" aria-hidden />
						Continue to selections
					</Button>
				</div>
			)}
			<Accordion
				type="multiple"
				className="w-full"
				defaultValue={Object.keys(resourceConfigs)}
			>
				{Object.entries(resourceConfigs).map(([typeKey, config]) => (
					<ResourceTypeAccordionItem
						key={typeKey}
						typeKey={typeKey}
						config={config}
						items={resources[typeKey] || []}
						extraProperties={extraProperties[typeKey] || []}
						onAdd={() => setOpenModalType(typeKey)}
						onRemoveResource={onRemoveResource}
					/>
				))}
			</Accordion>

			{openModalType && (
				<AddResourceModal
					open
					resourceType={openModalType}
					displayName={
						resourceConfigs[openModalType]?.displayName ?? openModalType
					}
					onClose={() => setOpenModalType(null)}
				/>
			)}
		</div>
	);
}
