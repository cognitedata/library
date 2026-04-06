import { useCallback, useRef, useState } from "react";
import { useI18n } from "@/shared/i18n";
import { useNavigation } from "@/shared/NavigationContext";
import { loadNavState, saveNavState } from "@/shared/nav-persistence";
import { DataModelUsage } from "./DataModelUsage";
import { TransformationsList } from "./Transformations";
import { TransformationOverlap } from "./TransformationOverlap";

type SubView = "list" | "overlap" | "dataModelUsage";

const SUB_VIEWS: SubView[] = ["list", "overlap", "dataModelUsage"];

function getInitialSubView(): SubView {
  const stored = loadNavState().transformationsSubView;
  return stored && SUB_VIEWS.includes(stored) ? stored : "list";
}

export function TransformationsPage() {
  const { t } = useI18n();
  const [subView, setSubViewState] = useState<SubView>(getInitialSubView);

  const setSubView = useCallback((next: SubView) => {
    setSubViewState(next);
    saveNavState({ transformationsSubView: next });
  }, []);
  const nav = useNavigation();
  const clearListSelectionRef = useRef<(() => void) | null>(null);

  return (
    <section className="flex flex-col gap-4">
      <header className="flex flex-wrap items-center justify-between gap-2 border-b border-slate-200 pb-3">
        <div className="flex flex-wrap items-center gap-2">
          <span className="text-sm font-medium text-slate-600">
            {t("transformations.title")}
          </span>
          <nav className="flex gap-1" aria-label={t("transformations.subNavLabel")}>
            <button
              type="button"
              onClick={() => {
                setSubView("list");
                clearListSelectionRef.current?.();
              }}
              className={`rounded-md px-3 py-1.5 text-sm font-medium ${
                subView === "list"
                  ? "bg-slate-200 text-slate-900"
                  : "text-slate-600 hover:bg-slate-100"
              }`}
            >
              {t("transformations.subView.list")}
            </button>
            <button
              type="button"
              onClick={() => setSubView("overlap")}
              className={`rounded-md px-3 py-1.5 text-sm font-medium ${
                subView === "overlap"
                  ? "bg-slate-200 text-slate-900"
                  : "text-slate-600 hover:bg-slate-100"
              }`}
            >
              {t("transformations.subView.overlap")}
            </button>
            <button
              type="button"
              onClick={() => setSubView("dataModelUsage")}
              className={`rounded-md px-3 py-1.5 text-sm font-medium ${
                subView === "dataModelUsage"
                  ? "bg-slate-200 text-slate-900"
                  : "text-slate-600 hover:bg-slate-100"
              }`}
            >
              {t("transformations.subView.dataModelUsage")}
            </button>
          </nav>
        </div>
      </header>
      {subView === "list" ? (
        <TransformationsList
          transformationToSelect={nav?.transformationToSelect ?? null}
          onTransformationSelected={nav?.clearTransformationToSelect}
          clearSelectionRef={clearListSelectionRef}
        />
      ) : subView === "overlap" ? (
        <TransformationOverlap />
      ) : (
        <DataModelUsage />
      )}
    </section>
  );
}
