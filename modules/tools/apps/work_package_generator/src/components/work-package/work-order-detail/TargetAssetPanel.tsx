import { Clock, RotateCcw } from "lucide-react";
import { Button } from "@/components/ui";
import type { Asset } from "@/types";
import { formatDate } from "@/utils/datetime";
import { BadgeRow, Row } from "../detail-row";

interface TargetAssetPanelProps {
	asset: Asset | null;
	assetErrorMessage: string | null;
	onRetryAssetResolution?: () => void;
}

export function TargetAssetPanel({
	asset,
	assetErrorMessage,
	onRetryAssetResolution,
}: TargetAssetPanelProps) {
	return (
		<div>
			<h4 className="text-xs font-semibold mb-2 text-muted-foreground uppercase tracking-wider">
				Target Asset
			</h4>
			{asset ? (
				<dl>
					<Row label="Name" value={asset.assetName} />
					<Row label="External ID" value={asset.assetExternalId} mono />
					<Row label="Description" value={asset.description} />
					<BadgeRow label="Status" value={asset.status} />
					<BadgeRow label="Criticality" value={asset.criticality} />
					<Row label="Type" value={asset.type} />
					<Row label="Asset Type" value={asset.assetType} />
					<Row label="Category" value={asset.category} />
					<Row label="Subcategory" value={asset.subcategory} />
					<Row label="Process" value={asset.process} />
					<Row label="Parent Asset" value={asset.parentAsset} />
					<Row label="Parent ID" value={asset.parentAssetExternalId} mono />
					<Row label="Hierarchy" value={asset.hierarchy} />
					<Row label="Location" value={asset.location} />
					<Row label="Area" value={asset.area} />
					<Row label="Zone" value={asset.zone} />
					<Row label="Building" value={asset.building} />
					<Row label="Floor" value={asset.floor} />
					<Row label="Room" value={asset.room} />
					<Row label="Manufacturer" value={asset.manufacturer} />
					<Row label="Model" value={asset.model} />
					<Row label="Serial No." value={asset.serialNumber} />
					<Row label="Installed" value={formatDate(asset.installationDate)} />
					<Row label="Capacity" value={asset.capacity} />
					<Row label="Power Rating" value={asset.powerRating} />
					<Row label="Pressure" value={asset.pressureRating} />
					<Row label="Temperature" value={asset.temperatureRating} />
					<Row label="Flow Rate" value={asset.flowRate} />
					<Row label="Material" value={asset.material} />
					<Row label="Dimensions" value={asset.dimensions} />
					<Row label="Weight" value={asset.weight} />
					<Row label="Op. Conditions" value={asset.operatingConditions} />
					<Row label="Specs" value={asset.specifications} />
				</dl>
			) : assetErrorMessage ? (
				<div className="flex flex-col gap-2 rounded-lg border border-dashed p-4 text-xs text-destructive">
					<p className="leading-snug">{assetErrorMessage}</p>
					{onRetryAssetResolution && (
						<Button
							type="button"
							variant="outline"
							size="sm"
							onClick={onRetryAssetResolution}
						>
							<RotateCcw className="h-3.5 w-3.5 shrink-0" />
							Retry asset resolution
						</Button>
					)}
				</div>
			) : (
				<div className="flex items-center gap-2 rounded-lg border border-dashed p-4 text-xs text-muted-foreground">
					<Clock className="h-4 w-4 shrink-0" />
					Resolving asset...
				</div>
			)}
		</div>
	);
}
