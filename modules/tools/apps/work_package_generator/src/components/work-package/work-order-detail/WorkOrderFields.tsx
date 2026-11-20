import { useAppConfig } from "@/app-config";
import type { WorkOrder } from "@/types";
import { formatDate } from "@/utils/datetime";
import { BadgeRow, Row } from "../detail-row";

interface WorkOrderFieldsProps {
	workOrder: WorkOrder;
}

export function WorkOrderFields({ workOrder }: WorkOrderFieldsProps) {
	const workOrderTerm = useAppConfig().ui.terms.workOrder;
	return (
		<div>
			<h4 className="text-xs font-semibold mb-2 text-muted-foreground uppercase tracking-wider">
				{workOrderTerm}
			</h4>
			<dl>
				<Row label="Name" value={workOrder.name} />
				<Row label="External ID" value={workOrder.externalId} mono />
				<Row label={`${workOrderTerm} #`} value={workOrder.workOrderNumber} />
				<Row label="Description" value={workOrder.description} />
				<BadgeRow label="Status" value={workOrder.status} />
				<BadgeRow label="Priority" value={workOrder.priority} />
				<BadgeRow label="Criticality" value={workOrder.criticality} />
				<Row label="Type" value={workOrder.type} />
				<Row label="WO Type" value={workOrder.workOrderType} />
				<Row label="Maint. Type" value={workOrder.maintenanceType} />
				<Row label="Category" value={workOrder.category} />
				<Row label="Frequency" value={workOrder.frequency} />
				<Row label="Requester" value={workOrder.requester} />
				<Row label="Assigned To" value={workOrder.assignedTo} />
				<Row label="Location" value={workOrder.location} />
				<Row label="Equipment" value={workOrder.equipment} />
				<Row label="System" value={workOrder.system} />
				<Row
					label="Sched. Start"
					value={formatDate(workOrder.scheduledStartTime)}
				/>
				<Row
					label="Sched. End"
					value={formatDate(workOrder.scheduledEndTime)}
				/>
				<Row
					label="Actual Start"
					value={formatDate(workOrder.actualStartTime)}
				/>
				<Row label="Actual End" value={formatDate(workOrder.actualEndTime)} />
				<Row
					label="Est. Duration"
					value={
						workOrder.estimatedDuration
							? `${workOrder.estimatedDuration} hrs`
							: undefined
					}
				/>
				<Row label="Source ID" value={workOrder.sourceId} mono />
			</dl>
		</div>
	);
}
