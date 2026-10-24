/**
 * Shared detail-row primitives used in work order / asset detail views.
 */

import { Badge } from "@/components/ui";

// ─── Status badge variant ─────────────────────────────────────────────────────

const ACTIVE_STATUSES = new Set([
	"active",
	"open",
	"in progress",
	"inprogress",
]);
const CLOSED_STATUSES = new Set([
	"closed",
	"cancelled",
	"canceled",
	"completed",
	"done",
]);
const CRITICAL_STATUSES = new Set(["critical", "high", "overdue"]);

export function statusVariant(
	value?: string
): "inProgress" | "secondary" | "error" | "gray" {
	const v = value?.toLowerCase() ?? "";
	if (ACTIVE_STATUSES.has(v)) return "inProgress";
	if (CLOSED_STATUSES.has(v)) return "secondary";
	if (CRITICAL_STATUSES.has(v)) return "error";
	return "gray";
}

// ─── Row components ───────────────────────────────────────────────────────────

interface RowProps {
	label: string;
	value?: string | number | null;
	mono?: boolean;
}

/** A key/value row that renders nothing when the value is empty. */
export function Row({ label, value, mono = false }: RowProps) {
	if (value === undefined || value === null || String(value).trim() === "")
		return null;
	return (
		<div className="flex items-start gap-2 py-1.5 border-b border-muted/30 last:border-0">
			<dt className="text-muted-foreground text-xs w-36 shrink-0 leading-relaxed">
				{label}
			</dt>
			<dd
				className={`text-xs flex-1 min-w-0 leading-relaxed wrap-break-word ${
					mono ? "font-mono text-[10px]" : ""
				}`}
			>
				{String(value)}
			</dd>
		</div>
	);
}

/** A key/badge row that renders a status-coloured badge, or nothing when empty. */
export function BadgeRow({ label, value }: { label: string; value?: string }) {
	if (!value) return null;
	return (
		<div className="flex items-center gap-2 py-1.5 border-b border-muted/30 last:border-0">
			<dt className="text-muted-foreground text-xs w-36 shrink-0">{label}</dt>
			<dd>
				<Badge variant={statusVariant(value)} className="text-[10px] px-2 py-0">
					{value}
				</Badge>
			</dd>
		</div>
	);
}
