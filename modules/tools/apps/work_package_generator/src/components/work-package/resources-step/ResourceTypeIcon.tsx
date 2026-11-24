import { Activity, Box, Cog, FileText, Wrench } from "lucide-react";

export function ResourceTypeIcon({ typeKey }: { typeKey: string }) {
	switch (typeKey) {
		case "file":
			return <FileText className="h-3.5 w-3.5" />;
		case "timeseries":
			return <Activity className="h-3.5 w-3.5" />;
		case "activity":
			return <Activity className="h-3.5 w-3.5" />;
		case "asset":
			return <Box className="h-3.5 w-3.5" />;
		case "equipment":
			return <Wrench className="h-3.5 w-3.5" />;
		default:
			return <Cog className="h-3.5 w-3.5" />;
	}
}
