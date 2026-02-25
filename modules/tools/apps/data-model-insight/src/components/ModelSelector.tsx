import { useState, useMemo } from "react";
import type { DataModelRef } from "@/hooks/useDataModels";
import type { DataModelSelection } from "@/hooks/useDataModelDoc";

interface ModelSelectorProps {
  models: DataModelRef[];
  isLoading: boolean;
  error: Error | null;
  onSelect: (selection: DataModelSelection) => void;
}

export function ModelSelector({ models, isLoading, error, onSelect }: ModelSelectorProps) {
  const [modelSearch, setModelSearch] = useState("");
  const [manualSpace, setManualSpace] = useState("");
  const [manualExternalId, setManualExternalId] = useState("");
  const [manualVersion, setManualVersion] = useState("");

  const filtered = useMemo(() => {
    const q = modelSearch.trim().toLowerCase();
    if (!q) return models;
    return models.filter(
      (m) =>
        (m.name ?? "").toLowerCase().includes(q) ||
        (m.externalId ?? "").toLowerCase().includes(q) ||
        (m.space ?? "").toLowerCase().includes(q)
    );
  }, [models, modelSearch]);

  const loadManual = () => {
    if (manualSpace.trim() && manualExternalId.trim() && manualVersion.trim()) {
      onSelect({
        space: manualSpace.trim(),
        externalId: manualExternalId.trim(),
        version: manualVersion.trim(),
      });
    }
  };

  return (
    <div className="min-h-screen bg-slate-950 text-slate-100 flex flex-col w-full">
      <div className="w-full flex-1 flex flex-col justify-center px-6 sm:px-8 lg:px-12 py-10 space-y-8">
        <header className="text-center">
          <h1 data-app-title className="text-2xl sm:text-3xl font-bold">CDF Data Model Documentation</h1>
          <p data-app-subtitle className="mt-1">Select a data model by name or externalId</p>
        </header>

        {error && (
          <div className="rounded-lg bg-red-950/50 border border-red-800 text-red-200 px-4 py-3 text-sm">
            {error.message}
          </div>
        )}

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 w-full">
          <section className="dm-card rounded-xl bg-slate-900/80 border border-slate-700 p-5 space-y-4 min-w-0">
            <h2 className="text-sm font-medium text-white/90 uppercase tracking-wide">Search data model</h2>
            <input
              type="search"
              placeholder="Search by model name or externalId…"
              value={modelSearch}
              onChange={(e) => setModelSearch(e.target.value)}
              className="w-full rounded-lg bg-slate-800 border border-slate-600 text-slate-100 px-3 py-2 text-sm placeholder-slate-500"
              disabled={isLoading}
            />
            {isLoading ? (
              <p className="text-slate-500 text-sm">Loading data models…</p>
            ) : (
              <ul className="max-h-80 overflow-y-auto space-y-1 rounded-lg bg-slate-800/50 border border-slate-700 p-2">
                {filtered.length === 0 ? (
                  <li className="text-slate-500 text-sm py-2">No data models found</li>
                ) : (
                  filtered.map((m) => (
                    <li key={`${m.space}|${m.externalId}|${m.version}`}>
                      <button
                        type="button"
                        onClick={() => onSelect({ space: m.space, externalId: m.externalId, version: m.version })}
                        className="w-full text-left px-3 py-2.5 rounded-md hover:bg-slate-700/80 text-slate-100 flex flex-col gap-0.5"
                      >
                        <span className="font-semibold text-white truncate">{m.name ?? m.externalId}</span>
                        <span className="text-xs text-slate-400">
                          Space: <span className="text-white/90">{m.space}</span>
                          {" · "}
                          Version: <span className="text-white/90">{m.version}</span>
                        </span>
                      </button>
                    </li>
                  ))
                )}
              </ul>
            )}
          </section>

          <section className="dm-card rounded-xl bg-slate-900/80 border border-slate-700 p-5 space-y-4 min-w-0">
            <h2 className="text-sm font-medium text-white/90 uppercase tracking-wide">Or enter manually</h2>
            <div className="grid grid-cols-1 sm:grid-cols-3 gap-3">
              <input
                type="text"
                placeholder="Space"
                value={manualSpace}
                onChange={(e) => setManualSpace(e.target.value)}
                className="rounded-lg bg-slate-800 border border-slate-600 text-slate-200 px-3 py-2 text-sm placeholder-slate-500"
              />
              <input
                type="text"
                placeholder="External ID"
                value={manualExternalId}
                onChange={(e) => setManualExternalId(e.target.value)}
                className="rounded-lg bg-slate-800 border border-slate-600 text-slate-200 px-3 py-2 text-sm placeholder-slate-500"
              />
              <input
                type="text"
                placeholder="Version"
                value={manualVersion}
                onChange={(e) => setManualVersion(e.target.value)}
                className="rounded-lg bg-slate-800 border border-slate-600 text-slate-200 px-3 py-2 text-sm placeholder-slate-500"
              />
            </div>
            <button
              type="button"
              onClick={loadManual}
              disabled={!manualSpace.trim() || !manualExternalId.trim() || !manualVersion.trim()}
              className="dm-btn-primary rounded-lg bg-amber-600 hover:bg-amber-500 disabled:opacity-50 disabled:pointer-events-none text-slate-900 font-medium px-4 py-2 text-sm"
            >
              Load documentation
            </button>
          </section>
        </div>
      </div>
    </div>
  );
}
