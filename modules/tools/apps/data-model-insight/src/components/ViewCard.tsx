import { useState } from "react";
import type { ViewInfo, PropertyInfo } from "@/types/dataModel";
import { groupPropertiesByContainer } from "@/lib/groupByContainer";
import { getPropertyTypeTextClass } from "@/lib/propertyTypeColor";

interface ViewCardProps {
  viewId: string;
  view: ViewInfo;
  allViews: Record<string, { name: string; display_name?: string; implements: string }>;
  icon: string;
  isDark: boolean;
  onOpenModal: (viewId: string) => void;
}

/** For header/titles: show "Name (externalId)" or just externalId */
function nameAndId(displayName: string | undefined, externalId: string): string {
  if (!externalId) return "—";
  if (displayName) return `${displayName} (${externalId})`;
  return externalId;
}

/** Show both name and externalId (two lines) for view/container property definitions */
function NameAndExternalIdCell({
  displayName,
  externalId,
  isDark,
}: {
  displayName: string | undefined;
  externalId: string;
  isDark: boolean;
}) {
  const muted = isDark ? "text-slate-400" : "text-slate-500";
  if (!externalId) return <span>—</span>;
  return (
    <div className="flex flex-col gap-0.5">
      <span className="font-medium">{displayName ?? externalId}</span>
      <span className={`text-xs font-mono ${muted}`}>{externalId}</span>
    </div>
  );
}

function PropertyTable({
  props: propList,
  allViews,
  isDark,
}: {
  props: PropertyInfo[];
  allViews: Record<string, { name: string; display_name?: string; implements: string }>;
  isDark: boolean;
}) {
  const th = isDark ? "border-slate-600 text-slate-300" : "border-slate-300 text-slate-600";
  const tdBorder = isDark ? "border-slate-700/50" : "border-slate-200";
  const tdMain = isDark ? "text-slate-200" : "text-slate-800";
  const tdMuted = isDark ? "text-slate-400" : "text-slate-600";
  const rel = isDark ? "text-amber-400" : "text-amber-600";
  return (
    <table className="w-full text-sm border-collapse table-fixed">
      <colgroup>
        <col style={{ width: "20%" }} />
        <col style={{ width: "20%" }} />
        <col style={{ width: "18%" }} />
        <col style={{ width: "8%" }} />
        <col />
      </colgroup>
      <thead>
        <tr className={`border-b ${th} text-left`}>
          <th className="py-2 px-2 text-left font-medium">View Property</th>
          <th className="py-2 px-2 text-left font-medium">Container Property</th>
          <th className="py-2 px-2 text-left font-medium">Type</th>
          <th className="py-2 px-2 text-left font-medium">Card.</th>
          <th className="py-2 px-2 text-left font-medium">Description</th>
        </tr>
      </thead>
      <tbody>
        {propList.map((p) => (
          <tr key={p.name} className={`border-b ${tdBorder}`}>
            <td className={`py-1.5 px-2 align-top break-words ${tdMain}`}>
              <NameAndExternalIdCell displayName={p.display_name} externalId={p.name} isDark={isDark} />
            </td>
            <td className={`py-1.5 px-2 align-top break-words ${tdMuted}`}>
              {p.container_property ? (
                <NameAndExternalIdCell
                  displayName={p.container_property_name ?? undefined}
                  externalId={p.container_property}
                  isDark={isDark}
                />
              ) : (
                "—"
              )}
            </td>
            <td className={`py-1.5 px-2 align-top break-words`}>
              {p.connection ? (
                <span className={`${rel} break-all`}>→ {nameAndId(allViews[p.type]?.display_name, p.type)}</span>
              ) : (
                <span className={getPropertyTypeTextClass(p.type, !!p.connection, isDark)}>{p.type}</span>
              )}
            </td>
            <td className={`py-1.5 px-2 align-top ${tdMuted} font-mono text-xs whitespace-nowrap`}>
              {p.connection === "direct" || p.min_count === "null" || p.max_count === "null" ? "null" : `${p.min_count}..${p.max_count}`}
            </td>
            <td className={`py-1.5 px-2 ${tdMuted} min-w-0`}>
              <span className="line-clamp-2 break-words" title={p.description ?? undefined}>{p.description ?? "—"}</span>
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

export function ViewCard({ viewId, view, allViews, icon, isDark, onOpenModal }: ViewCardProps) {
  const [expanded, setExpanded] = useState(false);
  const ownProps = view.properties.filter((p) => !p.inherited_from);
  const inheritedBySource = view.properties
    .filter((p) => p.inherited_from)
    .reduce<Record<string, PropertyInfo[]>>((acc, p) => {
      const src = p.inherited_from ?? "?";
      if (!acc[src]) acc[src] = [];
      acc[src].push(p);
      return acc;
    }, {});

  const title = nameAndId(view.display_name, viewId);
  const impl = view.implements ? view.implements.split(",").slice(0, 2).join(", ") : "Base";

  const cardBg = isDark ? "bg-slate-800/80 border-slate-600" : "bg-white border-slate-200";
  const cardHover = isDark ? "hover:bg-slate-700/50" : "hover:bg-slate-100";
  const cardExpandBg = isDark ? "bg-slate-900/50 border-slate-600" : "bg-slate-50 border-slate-200";
  const titleCls = isDark ? "text-slate-100" : "text-slate-900";
  const descCls = isDark ? "text-slate-400" : "text-slate-600";
  const metaCls = isDark ? "text-slate-500" : "text-slate-500";
  const sectionTitleCls = isDark ? "text-slate-400" : "text-slate-600";

  return (
    <div className={`rounded-xl border overflow-hidden ${cardBg} shadow-sm`}>
      <button
        type="button"
        onClick={() => setExpanded(!expanded)}
        className={`w-full flex items-start gap-3 p-4 text-left transition-colors ${cardHover}`}
      >
        <span className="text-2xl shrink-0">{icon}</span>
        <div className="min-w-0 flex-1">
          <div className={`font-medium ${titleCls}`}>{title}</div>
          <div className={`${descCls} text-sm mt-0.5 line-clamp-2`}>{view.description || "No description"}</div>
          <div className={`flex flex-wrap gap-2 mt-2 text-xs ${metaCls}`}>
            <span>extends {impl}</span>
            <span>{view.own_property_count} + {view.inherited_property_count} props</span>
          </div>
        </div>
        <div className="flex items-center gap-1 shrink-0">
          <button
            type="button"
            onClick={(e) => { e.stopPropagation(); onOpenModal(viewId); }}
            className={`p-1.5 rounded ${isDark ? "hover:bg-slate-600 text-slate-400" : "hover:bg-slate-200 text-slate-600"}`}
            title="Expand in pop-out"
            aria-label="Expand"
          >
            <svg width="18" height="18" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
              <path d="M15 3h6v6M9 21H3v-6M21 3l-7 7M3 21l7-7" />
            </svg>
          </button>
          <span className={metaCls}>{expanded ? "▼" : "▶"}</span>
        </div>
      </button>
      {expanded && (
        <div className={`border-t p-4 space-y-4 ${cardExpandBg}`}>
          {ownProps.length > 0 && (
            <div className="space-y-4">
              <div className={`text-sm font-medium ${sectionTitleCls}`}>Own properties ({ownProps.length})</div>
              {groupPropertiesByContainer(ownProps, allViews).map(({ label, props: sectionProps }) => (
                <div key={label}>
                  <div className={`text-xs font-medium mb-1.5 ${sectionTitleCls} opacity-90`}>In {label} ({sectionProps.length})</div>
                  <PropertyTable props={sectionProps} allViews={allViews} isDark={isDark} />
                </div>
              ))}
            </div>
          )}
          {Object.entries(inheritedBySource).map(([source, props]) => (
            <div key={source} className="space-y-4">
              <div className={`text-sm font-medium ${sectionTitleCls}`}>From {nameAndId(allViews[source]?.display_name, source)} ({props.length})</div>
              {groupPropertiesByContainer(props, allViews).map(({ label, props: sectionProps }) => (
                <div key={`${source}-${label}`}>
                  <div className={`text-xs font-medium mb-1.5 ${sectionTitleCls} opacity-90`}>In {label} ({sectionProps.length})</div>
                  <PropertyTable props={sectionProps} allViews={allViews} isDark={isDark} />
                </div>
              ))}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
