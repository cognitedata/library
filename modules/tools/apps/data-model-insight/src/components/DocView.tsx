import { useState, useMemo, useRef, useEffect } from "react";
import type { DocModel } from "@/types/dataModel";
import { getCategoriesAndLabels } from "@/lib/cdfToDocModel";
import { getPropertyTypeTextClass } from "@/lib/propertyTypeColor";
import {
  buildSearchData,
  searchViews,
  searchProperties,
  type ViewSearchHit,
  type PropertySearchHit,
} from "@/lib/searchData";
import { ViewCard } from "./ViewCard";
import { ViewModal } from "./ViewModal";
import { SvgDiagram } from "./SvgDiagram";
import { DiagramPane, DIAGRAM_PANE_TOOLBAR_HEIGHT } from "./DiagramPane";
import { getOrganicDiagramSpecs } from "@/lib/diagramData";

interface DocViewProps {
  doc: DocModel;
  onBack: () => void;
}

export function DocView({ doc, onBack }: DocViewProps) {
  const [activeSection, setActiveSection] = useState<string>("overview");
  const [viewSearch, setViewSearch] = useState("");
  const [propSearch, setPropSearch] = useState("");
  const [selectedViewFilter, setSelectedViewFilter] = useState<string | null>(null);
  const [selectedPropFilter, setSelectedPropFilter] = useState<{ viewId: string; viewProperty: string } | null>(null);
  const [theme, setTheme] = useState<"dark" | "light">("dark");
  const [modalViewId, setModalViewId] = useState<string | null>(null);

  const { categories, categoryLabels, viewDomains } = useMemo(() => getCategoriesAndLabels(doc), [doc]);
  const categoryIds = useMemo(() => Object.keys(categories).sort(), [categories]);
  const topicDiagramSpecs = useMemo(
    () => getOrganicDiagramSpecs(doc, categories, categoryLabels),
    [doc, categories, categoryLabels]
  );

  const [openDiagramTitles, setOpenDiagramTitles] = useState<Set<string>>(() => new Set());
  useEffect(() => {
    if (activeSection === "diagrams" && topicDiagramSpecs.length > 0) {
      setOpenDiagramTitles(new Set(topicDiagramSpecs.map((s) => s.title)));
    }
  }, [activeSection, topicDiagramSpecs]);

  const searchData = useMemo(
    () => buildSearchData(doc, viewDomains, categoryLabels),
    [doc, viewDomains, categoryLabels]
  );

  const viewSearchResults = useMemo(() => {
    const viewQuery = viewSearch.includes("*") ? "" : viewSearch;
    const propFilterForViews = selectedPropFilter?.viewProperty ??
      (viewSearch.includes("*") ? propSearch.trim().replace(/\*/g, "").trim() : undefined);
    return searchViews(searchData, viewQuery, propFilterForViews);
  }, [searchData, viewSearch, propSearch, selectedPropFilter]);

  const viewIdsFromViewSearch = useMemo(
    () => new Set(viewSearchResults.map((r) => r.viewId)),
    [viewSearchResults]
  );

  const propSearchResults = useMemo(() => {
    const propQuery = propSearch.includes("*") ? propSearch.trim().replace(/\*/g, "").trim() : propSearch;
    const filterByViewIds = selectedPropFilter
      ? viewIdsFromViewSearch
      : propSearch.includes("*") && viewIdsFromViewSearch.size > 0
        ? viewIdsFromViewSearch
        : undefined;
    const filterByViewId = selectedViewFilter && !selectedPropFilter ? selectedViewFilter : undefined;
    return searchProperties(searchData, propQuery, filterByViewId, filterByViewIds);
  }, [searchData, propSearch, selectedViewFilter, selectedPropFilter, viewIdsFromViewSearch]);

  const filteredViewIds = useMemo(() => {
    if (!viewSearch.trim() && !propSearch.trim() && !selectedViewFilter && !selectedPropFilter) return null;
    if (selectedViewFilter) return [selectedViewFilter];
    if (selectedPropFilter) return viewSearchResults.map((r) => r.viewId);
    const fromViewSearch =
      viewSearch.trim() ? viewSearchResults.map((r) => r.viewId) : Object.keys(doc.views);
    const fromPropSearch =
      propSearch.trim() ? [...new Set(propSearchResults.map((r) => r.viewId))] : Object.keys(doc.views);
    const viewSet = new Set(fromViewSearch.filter((id) => fromPropSearch.includes(id)));
    return viewSet.size > 0 ? Array.from(viewSet) : null;
  }, [viewSearch, propSearch, viewSearchResults, propSearchResults, selectedViewFilter, selectedPropFilter, doc.views]);

  const totalViews = Object.keys(doc.views).length;
  const totalProps = Object.values(doc.views).reduce((s, v) => s + v.properties.length, 0);
  const totalRelations = doc.direct_relations.length;

  const headerRef = useRef<HTMLElement>(null);
  const mainRef = useRef<HTMLElement>(null);
  const [diagramEstate, setDiagramEstate] = useState<{ width: number; height: number } | null>(null);

  useEffect(() => {
    const el = headerRef.current;
    if (!el) return;
    const setHeight = () => {
      document.documentElement.style.setProperty("--dm-header-height", `${el.offsetHeight}px`);
    };
    setHeight();
    const ro = new ResizeObserver(setHeight);
    ro.observe(el);
    return () => ro.disconnect();
  }, [doc.metadata.description]);

  useEffect(() => {
    const el = mainRef.current;
    if (!el) return;
    const setEstate = () => {
      const w = el.clientWidth;
      const h = el.clientHeight;
      if (w > 0 && h > 0)
        setDiagramEstate({
          width: w,
          height: Math.max(200, h - DIAGRAM_PANE_TOOLBAR_HEIGHT),
        });
    };
    setEstate();
    const ro = new ResizeObserver(setEstate);
    ro.observe(el);
    return () => ro.disconnect();
  }, [doc]);

  const isDark = theme === "dark";
  const bg = isDark ? "bg-slate-950" : "bg-slate-100";
  const text = isDark ? "text-white" : "text-slate-900";
  const textMuted = isDark ? "text-slate-400" : "text-slate-600";
  const border = isDark ? "border-slate-700" : "border-slate-200";
  const inputBg = isDark ? "bg-slate-800 border-slate-600 text-slate-100" : "bg-white border-slate-300 text-slate-900";

  const handleViewClick = (viewId: string) => {
    setSelectedViewFilter((prev) => (prev === viewId ? null : viewId));
    setSelectedPropFilter(null);
    setActiveSection(categoryIds.find((c) => categories[c]?.includes(viewId)) ?? "overview");
  };

  const handlePropClick = (viewId: string, viewProperty: string) => {
    setSelectedPropFilter((prev) =>
      prev?.viewId === viewId && prev?.viewProperty === viewProperty ? null : { viewId, viewProperty }
    );
    setSelectedViewFilter(null);
    setActiveSection(categoryIds.find((c) => categories[c]?.includes(viewId)) ?? "overview");
  };

  return (
    <div className={`flex flex-col h-screen w-full overflow-hidden ${bg} ${text}`}>
      {modalViewId && doc.views[modalViewId] && (
        <ViewModal
          viewId={modalViewId}
          view={doc.views[modalViewId]}
          allViews={doc.all_views}
          isDark={isDark}
          onClose={() => setModalViewId(null)}
        />
      )}
      <header ref={headerRef} className="sticky top-0 z-[60] shadow-lg">
        <div className="bg-[#0f172a] px-4 py-4">
          <div className="w-full flex flex-wrap items-center justify-between gap-4">
            <div className="min-w-0">
              <h1 data-app-title className="text-2xl sm:text-3xl md:text-4xl font-extrabold tracking-tight">
                {doc.metadata.name}
              </h1>
              <p data-app-subtitle className="text-sm mt-1.5 font-medium">
                {doc.metadata.space} / {doc.metadata.externalId} @ {doc.metadata.version}
              </p>
            </div>
            <div className="flex flex-wrap items-center gap-6 gap-y-2">
              <div className="flex gap-5 sm:gap-6">
                <div className="text-center dm-stat">
                  <div className="text-xl sm:text-2xl font-bold text-amber-500">{totalViews}</div>
                  <div className="text-xs text-white/80">Views</div>
                </div>
                <div className="text-center dm-stat">
                  <div className="text-xl sm:text-2xl font-bold text-amber-500">{totalProps}</div>
                  <div className="text-xs text-white/80">Properties</div>
                </div>
                <div className="text-center dm-stat">
                  <div className="text-xl sm:text-2xl font-bold text-amber-500">{totalRelations}</div>
                  <div className="text-xs text-white/80">Relations</div>
                </div>
              </div>
              <div className="flex items-center gap-2">
                <button
                type="button"
                onClick={() => setTheme(isDark ? "light" : "dark")}
                className="dm-nav-pill rounded-lg px-3 py-1.5 text-sm font-medium bg-white/20 text-white hover:bg-white/30"
              >
                {isDark ? "☀ Light" : "🌙 Dark"}
              </button>
              <button
                type="button"
                onClick={onBack}
                className="dm-btn-primary dm-nav-pill rounded-lg px-3 py-1.5 text-sm bg-amber-400 text-slate-900 font-semibold hover:bg-amber-300"
              >
                ← Back
                </button>
              </div>
            </div>
          </div>
        </div>
        {doc.metadata.description && (
          <p className={`w-full px-4 py-3 text-sm border-b ${isDark ? "bg-slate-900 text-slate-200 border-slate-700" : "bg-slate-100 text-slate-700 border-slate-200"}`}>
            {doc.metadata.description}
          </p>
        )}
      </header>

      <div className={`w-full px-4 py-4 border-b ${border} ${isDark ? "bg-slate-900/30" : "bg-slate-50"}`}>
        <p className="text-xs text-amber-500 mb-3">
          Search by name, description, or externalId. Filters apply across both columns. Use * to see all available options.
        </p>
        <div className="grid grid-cols-1 md:grid-cols-2 gap-x-6 gap-y-4 items-start">
          <div className="flex flex-col gap-1 min-w-0">
            <label className={`text-xs font-medium uppercase tracking-wide ${textMuted}`}>
              Search view types
            </label>
            <input
              type="search"
              placeholder="e.g. Asset, Equipment… Use * to show views matching property search"
              value={viewSearch}
              onChange={(e) => setViewSearch(e.target.value)}
              className={`rounded-lg border px-3 py-2 text-sm ${inputBg} placeholder-slate-500`}
            />
          </div>
          <div className="flex flex-col gap-1 min-w-0">
            <label className={`text-xs font-medium uppercase tracking-wide ${textMuted}`}>
              Search properties
            </label>
            <input
              type="search"
              placeholder="e.g. name, description… Use * to show properties from view search"
              value={propSearch}
              onChange={(e) => setPropSearch(e.target.value)}
              className={`rounded-lg border px-3 py-2 text-sm ${inputBg} placeholder-slate-500`}
            />
          </div>

          {(viewSearch.trim() || propSearch.trim() || selectedViewFilter || selectedPropFilter) && (
            <>
              <div
                className={`min-w-0 min-h-[7rem] flex flex-col rounded-lg border p-3 self-start ${(viewSearch.trim() || selectedPropFilter) ? (isDark ? "bg-slate-800/80 border-slate-600" : "bg-white border-slate-200 shadow-sm") : "border-transparent"}`}
                role="region"
                aria-label="View type search results"
              >
                {(viewSearch.trim() || selectedPropFilter) ? (
                  <>
                    <div className="flex items-center justify-between gap-2 flex-shrink-0">
                      <span className={`text-xs font-medium ${textMuted}`}>{viewSearchResults.length} view(s)</span>
                      {selectedViewFilter && (
                        <button
                          type="button"
                          onClick={() => setSelectedViewFilter(null)}
                          className="text-xs text-amber-500 hover:underline whitespace-nowrap"
                        >
                          Clear view filter
                        </button>
                      )}
                    </div>
                    <ul className="mt-1.5 max-h-72 overflow-y-auto space-y-0.5 flex-1 min-h-0">
                      {viewSearchResults.slice(0, 50).map((r: ViewSearchHit) => (
                        <li key={r.viewId}>
                          <button
                            type="button"
                            onClick={() => handleViewClick(r.viewId)}
                            className={`text-left text-sm w-full px-2 py-1 rounded ${isDark ? "hover:bg-slate-700" : "hover:bg-slate-200"} ${selectedViewFilter === r.viewId && !selectedPropFilter ? "ring-1 ring-amber-500" : ""}`}
                          >
                            <span className="font-medium">{r.displayName}</span>
                            {r.viewId !== r.displayName && (
                              <span className={`ml-1 ${textMuted}`}>({r.viewId})</span>
                            )}
                          </button>
                        </li>
                      ))}
                      {viewSearchResults.length > 50 && (
                        <li className={`text-xs ${textMuted}`}>+{viewSearchResults.length - 50} more</li>
                      )}
                    </ul>
                  </>
                ) : (
                  <span className={`text-xs ${textMuted}`}>—</span>
                )}
              </div>
              <div
                className={`min-w-0 min-h-[7rem] flex flex-col rounded-lg border p-3 self-start ${(propSearch.trim() || selectedViewFilter) ? (isDark ? "bg-slate-800/80 border-slate-600" : "bg-white border-slate-200 shadow-sm") : "border-transparent"}`}
                role="region"
                aria-label="Property search results"
              >
                {(propSearch.trim() || selectedViewFilter) ? (
                  <>
                    <div className="flex items-center justify-between gap-2 flex-shrink-0">
                      <span className={`text-xs font-medium ${textMuted}`}>{propSearchResults.length} property match(es)</span>
                      {selectedPropFilter && (
                        <button
                          type="button"
                          onClick={() => setSelectedPropFilter(null)}
                          className="text-xs text-amber-500 hover:underline whitespace-nowrap"
                        >
                          Clear property filter
                        </button>
                      )}
                    </div>
                    <ul className="mt-1.5 max-h-72 overflow-y-auto space-y-0.5 flex-1 min-h-0">
                      {propSearchResults.slice(0, 50).map((r: PropertySearchHit, i: number) => (
                        <li key={`${r.viewId}-${r.viewProperty}-${i}`}>
                          <button
                            type="button"
                            onClick={() => handlePropClick(r.viewId, r.viewProperty)}
                            className={`text-left text-sm w-full px-2 py-1 rounded ${isDark ? "hover:bg-slate-700" : "hover:bg-slate-200"} ${selectedPropFilter?.viewId === r.viewId && selectedPropFilter?.viewProperty === r.viewProperty ? "ring-1 ring-amber-500" : ""}`}
                          >
                            <span className={`font-mono font-medium ${getPropertyTypeTextClass(r.type, r.isRelation, isDark)}`}>
                              {r.viewPropertyName && r.viewPropertyName !== r.viewProperty
                                ? `${r.viewPropertyName} (${r.viewProperty})`
                                : r.viewProperty}
                            </span>
                            <span className={`ml-1.5 ${textMuted} font-mono text-xs`} title="value type">
                              · {r.type || "—"}
                            </span>
                            {r.containerProperty && (
                              <span className={`ml-1 ${textMuted}`}>→ {r.containerProperty}</span>
                            )}
                            <span className={`ml-1 ${textMuted}`}>in {r.viewDisplayName}</span>
                            {r.isRelation && r.type && (
                              <span className={`ml-1 ${textMuted}`}>
                                → target: {r.typeDisplay && r.typeDisplay !== r.type ? `${r.typeDisplay} (${r.type})` : r.type}
                              </span>
                            )}
                          </button>
                        </li>
                      ))}
                      {propSearchResults.length > 50 && (
                        <li className={`text-xs ${textMuted}`}>+{propSearchResults.length - 50} more</li>
                      )}
                    </ul>
                  </>
                ) : (
                  <span className={`text-xs ${textMuted}`}>—</span>
                )}
              </div>
            </>
          )}
        </div>
      </div>

      <nav
        className={`w-full px-4 py-2 flex flex-wrap gap-1 border-b ${border} ${isDark ? "bg-slate-900/20" : "bg-white/50"}`}
      >
        <button
          type="button"
          onClick={() => setActiveSection("overview")}
          className={`dm-nav-pill px-3 py-1.5 text-sm font-medium rounded-lg ${activeSection === "overview" ? "bg-amber-500 text-slate-900" : textMuted} hover:opacity-90`}
        >
          Overview
        </button>
        {topicDiagramSpecs.length > 0 && (
          <button
            type="button"
            onClick={() => setActiveSection("diagrams")}
            className={`dm-nav-pill px-3 py-1.5 text-sm font-medium rounded-lg ${activeSection === "diagrams" ? "bg-amber-500 text-slate-900" : textMuted} hover:opacity-90`}
          >
            Diagrams ({topicDiagramSpecs.length})
          </button>
        )}
        {categoryIds.map((cat) => (
          <button
            key={cat}
            type="button"
            onClick={() => setActiveSection(cat)}
            className={`dm-nav-pill px-3 py-1.5 text-sm font-medium rounded-lg ${activeSection === cat ? "bg-amber-500 text-slate-900" : textMuted} hover:opacity-90`}
          >
            {categoryLabels[cat]?.icon} {categoryLabels[cat]?.displayName ?? cat} ({categories[cat]?.length ?? 0})
          </button>
        ))}
      </nav>

      <main ref={mainRef} className={`w-full flex flex-col flex-1 min-h-0 overflow-auto px-4 py-4 dm-main-pane ${bg}`}>
        {activeSection === "overview" && (
          <section className="flex flex-col flex-1 min-h-0 w-full" style={{ minHeight: "30vh" }}>
            <div className="flex flex-col flex-1 min-h-0 w-full">
                <DiagramPane
                  className="flex-1 min-h-0 w-full"
                  style={{ minHeight: "280px" }}
                  baseWidth={diagramEstate?.width ?? 800}
                  baseHeight={diagramEstate?.height ?? 500}
                >
                  <SvgDiagram
                    doc={doc}
                    isDark={isDark}
                    className="w-full h-full rounded-xl border p-2 flex-1 min-h-0"
                    onViewClick={setModalViewId}
                    layoutWidth={diagramEstate?.width}
                    layoutHeight={diagramEstate?.height}
                  />
                </DiagramPane>
            </div>
          </section>
        )}

        {activeSection === "diagrams" && topicDiagramSpecs.length > 0 && (
          <section className="flex flex-col flex-1 min-h-0 overflow-auto">
            <div className="flex items-center gap-2 flex-shrink-0 mb-3">
              <button
                type="button"
                onClick={() => setOpenDiagramTitles(new Set(topicDiagramSpecs.map((s) => s.title)))}
                className="rounded border border-slate-500 bg-slate-700/50 text-slate-200 px-3 py-1.5 text-sm hover:bg-slate-600"
              >
                Open all
              </button>
              <button
                type="button"
                onClick={() => setOpenDiagramTitles(new Set())}
                className="rounded border border-slate-500 bg-slate-700/50 text-slate-200 px-3 py-1.5 text-sm hover:bg-slate-600"
              >
                Close all
              </button>
            </div>
            <div className="space-y-4 pb-4">
              {topicDiagramSpecs.map((spec) => {
                const isOpen = openDiagramTitles.has(spec.title);
                return (
                  <details
                    key={spec.title}
                    open={isOpen}
                    className="group rounded-xl border border-slate-600/50 bg-slate-800/30 overflow-hidden"
                  >
                    <summary
                      className="list-none cursor-pointer px-4 py-3 text-sm font-medium text-slate-200 hover:bg-slate-700/30 flex items-center justify-between [&::-webkit-details-marker]:hidden"
                      onClick={(e) => {
                        e.preventDefault();
                        setOpenDiagramTitles((prev) => {
                          const next = new Set(prev);
                          if (next.has(spec.title)) next.delete(spec.title);
                          else next.add(spec.title);
                          return next;
                        });
                      }}
                    >
                      <span>{spec.title}</span>
                      <span className="text-slate-500 text-xs font-normal">{spec.viewIds.length} views</span>
                    </summary>
                    <div className="px-4 pb-4 pt-0">
                      {spec.description && (
                        <p className={`text-xs mb-3 ${isDark ? "text-slate-400" : "text-slate-500"}`}>
                          {spec.description}
                        </p>
                      )}
                      <div className="flex flex-col min-h-[360px]">
                        <DiagramPane
                          className="min-h-[360px] flex-1 min-h-0"
                          baseWidth={diagramEstate?.width ?? 800}
                          baseHeight={diagramEstate?.height ?? 500}
                        >
                          <SvgDiagram
                            doc={doc}
                            isDark={isDark}
                            viewIds={spec.viewIds}
                            centerViewIds={spec.centerViewIds}
                            className="w-full h-full rounded-xl border p-2 flex-1 min-h-0"
                            onViewClick={setModalViewId}
                            layoutWidth={diagramEstate?.width}
                            layoutHeight={diagramEstate?.height}
                          />
                        </DiagramPane>
                      </div>
                    </div>
                  </details>
                );
              })}
            </div>
          </section>
        )}

        {categoryIds.includes(activeSection) && (
          <section className="space-y-4">
            <p className={textMuted}>{categoryLabels[activeSection]?.description}</p>
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 w-full items-start [&>*]:min-w-0">
              {(
                filteredViewIds
                  ? (categories[activeSection] ?? []).filter((id) => filteredViewIds.includes(id))
                  : (categories[activeSection] ?? [])
              ).length === 0 ? (
                <div className={`col-span-full dm-empty-state ${textMuted}`}>
                  No views in this category match the current filters. Clear the search or try another category.
                </div>
              ) : (
                (
                  filteredViewIds
                    ? (categories[activeSection] ?? []).filter((id) => filteredViewIds.includes(id))
                    : (categories[activeSection] ?? [])
                ).map((viewId) => {
                  const view = doc.views[viewId];
                  if (!view) return null;
                  return (
                    <div key={viewId} className="min-w-0 dm-card">
                      <ViewCard
                        viewId={viewId}
                        view={view}
                        allViews={doc.all_views}
                        icon={categoryLabels[activeSection]?.icon ?? "📦"}
                        isDark={isDark}
                        onOpenModal={setModalViewId}
                      />
                    </div>
                  );
                })
              )}
            </div>
          </section>
        )}
      </main>
    </div>
  );
}
