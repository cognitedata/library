import { useState } from "react";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { ApiError } from "@/shared/ApiError";
import { formatIso } from "@/shared/time-utils";
import { classifyHealth, isFailed, uptimeBg, uptimeColor } from "./uptime";
import type { ResourceHealth, ResourceReport } from "./types";

type Props = {
  report: ResourceReport;
  thresholdPct: number;
  loading: boolean;
};

function StatusBadge({ status }: { status?: string }) {
  if (!status) {
    return <span className="rounded-sm bg-slate-100 px-2 py-0.5 text-xs text-slate-500">—</span>;
  }
  const failed = isFailed(status);
  const cls = failed
    ? "bg-red-100 text-red-700"
    : "bg-emerald-100 text-emerald-700";
  return <span className={`rounded-sm px-2 py-0.5 text-xs font-medium ${cls}`}>{status}</span>;
}

function HealthPill({ resource, thresholdPct }: { resource: ResourceHealth; thresholdPct: number }) {
  const cls = classifyHealth(resource.runsInWindow, resource.uptimePercentage, thresholdPct);
  if (cls === "no_runs") {
    return (
      <span className="rounded-sm bg-slate-100 px-2 py-0.5 text-xs text-slate-600">No runs</span>
    );
  }
  if (cls === "healthy") {
    return (
      <span className="rounded-sm bg-emerald-100 px-2 py-0.5 text-xs font-medium text-emerald-700">
        Healthy
      </span>
    );
  }
  return (
    <span className="rounded-sm bg-red-100 px-2 py-0.5 text-xs font-medium text-red-700">
      Unhealthy
    </span>
  );
}

function ResourceRow({
  resource,
  thresholdPct,
}: {
  resource: ResourceHealth;
  thresholdPct: number;
}) {
  const [expanded, setExpanded] = useState(false);
  return (
    <>
      <tr className="border-t border-slate-200 hover:bg-slate-50">
        <td className="px-3 py-2">
          <button
            type="button"
            className="cursor-pointer text-left font-medium text-slate-900 hover:underline"
            onClick={() => setExpanded((v) => !v)}
          >
            {resource.name}
          </button>
          {resource.externalId && resource.externalId !== resource.name ? (
            <div className="text-xs text-slate-500">{resource.externalId}</div>
          ) : null}
        </td>
        <td className="px-3 py-2">
          <StatusBadge status={resource.lastStatus} />
        </td>
        <td className="px-3 py-2 text-xs text-slate-600">
          {resource.lastRunMs ? formatIso(resource.lastRunMs) : "—"}
        </td>
        <td className="px-3 py-2 text-right">
          <span className={`font-mono font-semibold ${uptimeColor(resource.uptimePercentage)}`}>
            {resource.runsInWindow > 0 ? `${resource.uptimePercentage.toFixed(1)}%` : "—"}
          </span>
        </td>
        <td className="px-3 py-2 text-right font-mono text-xs text-slate-600">
          {resource.successful}/{resource.successful + resource.failed}
        </td>
        <td className="px-3 py-2">
          <HealthPill resource={resource} thresholdPct={thresholdPct} />
        </td>
        <td className="px-3 py-2 text-right">
          {resource.fusionUrl ? (
            <a
              href={resource.fusionUrl}
              target="_blank"
              rel="noreferrer"
              className="text-xs text-slate-600 underline decoration-dotted hover:text-slate-900"
            >
              Open
            </a>
          ) : null}
        </td>
      </tr>
      {expanded ? (
        <tr className="border-t border-slate-100 bg-slate-50">
          <td colSpan={7} className="px-3 py-3">
            {resource.recentRuns.length === 0 ? (
              <div className="text-xs text-slate-500">No recent runs in window.</div>
            ) : (
              <ul className="space-y-1 text-xs">
                {resource.recentRuns.map((run, idx) => (
                  <li key={idx} className="flex items-start gap-3">
                    <StatusBadge status={run.status} />
                    <span className="font-mono text-slate-600">
                      {run.timeMs ? formatIso(run.timeMs) : "—"}
                    </span>
                    {run.message ? (
                      <span className="text-slate-700">{run.message}</span>
                    ) : null}
                  </li>
                ))}
              </ul>
            )}
          </td>
        </tr>
      ) : null}
    </>
  );
}

export function ResourceHealthPanel({ report, thresholdPct, loading }: Props) {
  const { kindLabel, resources, summary, error } = report;
  return (
    <Card>
      <CardHeader className="flex-row items-start justify-between gap-4">
        <div className="flex flex-col gap-1">
          <CardTitle>{kindLabel}s</CardTitle>
          <CardDescription>
            Uptime = successful / (successful + failed) within the selected time range. Threshold: {thresholdPct}%.
          </CardDescription>
        </div>
        <div className={`rounded-md border px-3 py-2 text-right text-sm ${uptimeBg(summary.aggregateUptime)}`}>
          <div className={`font-mono text-lg font-semibold ${uptimeColor(summary.aggregateUptime)}`}>
            {summary.total > 0 ? `${summary.aggregateUptime.toFixed(1)}%` : "—"}
          </div>
          <div className="text-xs text-slate-600">
            {summary.healthy} healthy · {summary.unhealthy} unhealthy · {summary.noRuns} no runs
          </div>
        </div>
      </CardHeader>
      <CardContent>
        {loading ? (
          <div className="text-sm text-slate-600">Loading {kindLabel.toLowerCase()}s…</div>
        ) : null}
        {error ? <ApiError message={error} /> : null}
        {!loading && !error ? (
          resources.length === 0 ? (
            <div className="rounded-md border border-slate-200 bg-slate-50 p-3 text-sm text-slate-600">
              No {kindLabel.toLowerCase()}s matched the filter.
            </div>
          ) : (
            <div className="max-h-80 overflow-auto rounded-md border border-slate-200">
              <table className="w-full text-sm">
                <thead className="sticky top-0 z-10 bg-slate-50 text-left text-xs uppercase tracking-wide text-slate-500 shadow-sm">
                  <tr>
                    <th className="px-3 py-2 font-medium">Name</th>
                    <th className="px-3 py-2 font-medium">Last status</th>
                    <th className="px-3 py-2 font-medium">Last run (UTC)</th>
                    <th className="px-3 py-2 text-right font-medium">Uptime %</th>
                    <th className="px-3 py-2 text-right font-medium">Success / Total</th>
                    <th className="px-3 py-2 font-medium">Health</th>
                    <th className="px-3 py-2 text-right font-medium">Fusion</th>
                  </tr>
                </thead>
                <tbody>
                  {resources.map((resource) => (
                    <ResourceRow
                      key={resource.id}
                      resource={resource}
                      thresholdPct={thresholdPct}
                    />
                  ))}
                </tbody>
              </table>
            </div>
          )
        ) : null}
      </CardContent>
    </Card>
  );
}
