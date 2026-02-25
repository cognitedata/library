import type { ViewInfo, PropertyInfo } from "@/types/dataModel";
import { groupPropertiesByContainer } from "@/lib/groupByContainer";
import { getPropertyTypeTextClass } from "@/lib/propertyTypeColor";

interface ViewModalProps {
  viewId: string;
  view: ViewInfo;
  allViews: Record<string, { name: string; display_name?: string; implements: string }>;
  isDark: boolean;
  onClose: () => void;
}

/** For header/titles: show "Name (externalId)" or just externalId */
function nameAndId(displayName: string | undefined, externalId: string): string {
  if (!externalId) return "—";
  if (displayName) return `${displayName} (${externalId})`;
  return externalId;
}

/** Cell that always shows both name and externalId on two lines (view/container property definitions). */
function NameAndExternalIdCell({
  displayName,
  externalId,
  isDark,
  className = "",
}: {
  displayName: string | undefined;
  externalId: string;
  isDark: boolean;
  className?: string;
}) {
  const muted = isDark ? "text-slate-400" : "text-slate-500";
  if (!externalId) return <span className={className}>—</span>;
  return (
    <div className={`flex flex-col gap-0.5 min-w-0 ${className}`}>
      <span className="font-medium truncate" title={displayName ?? externalId}>
        {displayName ?? externalId}
      </span>
      <span className={`text-xs font-mono ${muted}`} title={externalId}>
        {externalId}
      </span>
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
  const thBg = isDark ? "bg-slate-800" : "bg-slate-100";
  const th = isDark ? "border-slate-600 text-slate-300" : "border-slate-200 text-slate-600";
  const tdBorder = isDark ? "border-slate-700/50" : "border-slate-200";
  const tdMain = isDark ? "text-slate-200" : "text-slate-800";
  const tdMuted = isDark ? "text-slate-400" : "text-slate-600";
  const rel = isDark ? "text-amber-400" : "text-amber-600";
  return (
    <div className="overflow-x-auto rounded-lg border border-inherit">
      <table className="w-full text-sm border-collapse table-fixed" style={{ minWidth: "640px" }}>
        <colgroup>
          <col style={{ width: "20%" }} />
          <col style={{ width: "20%" }} />
          <col style={{ width: "18%" }} />
          <col style={{ width: "8%" }} />
          <col />
        </colgroup>
        <thead>
          <tr className={`border-b-2 ${th} text-left sticky top-0 z-10 ${thBg}`}>
            <th className={`py-3 px-4 text-left font-semibold ${thBg}`}>View Property</th>
            <th className={`py-3 px-4 text-left font-semibold ${thBg}`}>Container Property</th>
            <th className={`py-3 px-4 text-left font-semibold ${thBg}`}>Type</th>
            <th className={`py-3 px-4 text-left font-semibold ${thBg}`}>Card.</th>
            <th className={`py-3 px-4 text-left font-semibold ${thBg}`}>Description</th>
          </tr>
        </thead>
        <tbody>
          {propList.map((p) => (
            <tr key={p.name} className={`border-b ${tdBorder} hover:bg-black/5 dark:hover:bg-white/5`}>
              <td className={`py-2.5 px-4 align-top ${tdMain} break-words`}>
                <NameAndExternalIdCell displayName={p.display_name} externalId={p.name} isDark={isDark} />
              </td>
              <td className={`py-2.5 px-4 align-top ${tdMuted} break-words`}>
                {p.container_property ? (
                  <NameAndExternalIdCell
                    displayName={p.container_property_name ?? undefined}
                    externalId={p.container_property}
                    isDark={isDark}
                  />
                ) : (
                  <span>—</span>
                )}
              </td>
              <td className={`py-2.5 px-4 align-top break-words`}>
                {p.connection ? (
                  <span className={`${rel} break-all`}>→ {nameAndId(allViews[p.type]?.display_name, p.type)}</span>
                ) : (
                  <span className={getPropertyTypeTextClass(p.type, !!p.connection, isDark)}>{p.type}</span>
                )}
              </td>
              <td className={`py-2.5 px-4 align-top ${tdMuted} font-mono text-xs whitespace-nowrap`}>
                {p.connection === "direct" || p.min_count === "null" || p.max_count === "null" ? "null" : `${p.min_count}..${p.max_count}`}
              </td>
              <td className={`py-2.5 px-4 ${tdMuted} min-w-0`}>
                <span className="line-clamp-3 break-words" title={p.description ?? undefined}>{p.description ?? "—"}</span>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export function ViewModal({ viewId, view, allViews, isDark, onClose }: ViewModalProps) {
  const title = nameAndId(view.display_name ?? view.name, viewId);
  const ownProps = view.properties.filter((p) => !p.inherited_from);
  const inheritedBySource = view.properties
    .filter((p) => p.inherited_from)
    .reduce<Record<string, PropertyInfo[]>>((acc, p) => {
      const src = p.inherited_from ?? "?";
      if (!acc[src]) acc[src] = [];
      acc[src].push(p);
      return acc;
    }, {});

  const overlay = isDark ? "bg-black/60" : "bg-black/40";
  const content = isDark ? "bg-slate-800 border-slate-600" : "bg-white border-slate-200";
  const titleCls = isDark ? "text-white" : "text-slate-900";
  const descCls = isDark ? "text-slate-300" : "text-slate-600";
  const sectionCls = isDark ? "text-slate-400" : "text-slate-600";

  return (
    <div
      className={`fixed inset-0 z-50 ${overlay} flex`}
      onClick={onClose}
      role="dialog"
      aria-modal="true"
      aria-labelledby="view-modal-title"
    >
      <div
        className={`dm-modal-pane fixed top-[var(--dm-header-height,11rem)] bottom-0 left-0 right-0 z-50 w-full max-w-full rounded-none border-2 border-amber-500 shadow-2xl flex flex-col overflow-hidden ${content}`}
        onClick={(e) => e.stopPropagation()}
      >
        <div className={`flex items-start justify-between gap-4 px-6 py-5 border-b flex-shrink-0 ${isDark ? "border-slate-600" : "border-slate-200"}`}>
          <div className="min-w-0 flex-1">
            <h2 id="view-modal-title" className={`text-2xl font-bold tracking-tight ${titleCls}`}>
              {title}
            </h2>
            <p className={`text-sm mt-1 font-mono ${isDark ? "text-slate-400" : "text-slate-500"}`}>
              externalId: {viewId}
            </p>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="rounded-lg bg-amber-500 text-slate-900 font-semibold px-4 py-2 text-sm hover:bg-amber-400 shrink-0"
          >
            Close
          </button>
        </div>
        {view.description && (
          <p className={`px-6 py-3 text-sm ${descCls} border-b ${isDark ? "border-slate-600 bg-slate-800/30" : "border-slate-200 bg-slate-50"}`}>
            {view.description}
          </p>
        )}
        <div className="flex-1 overflow-y-auto min-h-0 px-6 py-5">
          <div className="max-w-[1600px] mx-auto space-y-8">
            {ownProps.length > 0 && (
              <section>
                <h3 className={`text-sm font-semibold uppercase tracking-wide mb-3 ${sectionCls}`}>
                  Own properties ({ownProps.length})
                </h3>
                <div className="space-y-6">
                  {groupPropertiesByContainer(ownProps, allViews).map(({ label, props: sectionProps }) => (
                    <div key={label}>
                      <h4 className={`text-xs font-semibold uppercase tracking-wide mb-2 ${sectionCls} opacity-90`}>
                        In {label} ({sectionProps.length})
                      </h4>
                      <PropertyTable props={sectionProps} allViews={allViews} isDark={isDark} />
                    </div>
                  ))}
                </div>
              </section>
            )}
            {Object.entries(inheritedBySource).map(([source, props]) => (
              <section key={source}>
                <h3 className={`text-sm font-semibold uppercase tracking-wide mb-3 ${sectionCls}`}>
                  From {nameAndId(allViews[source]?.display_name, source)} ({props.length})
                </h3>
                <div className="space-y-6">
                  {groupPropertiesByContainer(props, allViews).map(({ label, props: sectionProps }) => (
                    <div key={`${source}-${label}`}>
                      <h4 className={`text-xs font-semibold uppercase tracking-wide mb-2 ${sectionCls} opacity-90`}>
                        In {label} ({sectionProps.length})
                      </h4>
                      <PropertyTable props={sectionProps} allViews={allViews} isDark={isDark} />
                    </div>
                  ))}
                </div>
              </section>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}
