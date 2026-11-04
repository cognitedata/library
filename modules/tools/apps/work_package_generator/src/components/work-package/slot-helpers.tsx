import { Activity, Box, Cog, FileText, Wrench } from "lucide-react";
import { getAppConfig } from "@/app-config";
import type { TemplateSlot } from "@/hooks";
import type { ResourceInstance } from "@/types";

export function getCandidatesForSlotType(
	type: string,
	candidatesByType: Record<string, ResourceInstance[]>
): ResourceInstance[] {
	return candidatesByType[type] ?? [];
}

export function SlotIcon({ category }: { category: TemplateSlot["category"] }) {
	switch (category) {
		case "asset":
			return <Box className="h-3 w-3 text-orange-500 shrink-0" />;
		case "workorder":
		case "activity":
			return <Activity className="h-3 w-3 text-purple-500 shrink-0" />;
		case "equipment":
			return <Wrench className="h-3 w-3 text-amber-500 shrink-0" />;
		case "file":
			return <FileText className="h-3 w-3 text-blue-500 shrink-0" />;
		case "timeseries":
			return <Activity className="h-3 w-3 text-green-500 shrink-0" />;
		default:
			return <Cog className="h-3 w-3 text-muted-foreground shrink-0" />;
	}
}

export function slotCategoryLabel(category: TemplateSlot["category"]): string {
	switch (category) {
		case "asset":
			return "Asset";
		case "workorder":
			return getAppConfig().ui.terms.workOrder;
		case "activity":
			return "Activity";
		case "equipment":
			return "Equipment";
		case "file":
			return "File";
		case "timeseries":
			return "Time Series";
		default:
			return "Other";
	}
}
