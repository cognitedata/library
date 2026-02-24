import { useI18n } from "@/shared/i18n";

export function ProcessingBubbleLegend() {
  const { t } = useI18n();
  return (
    <div className="grid gap-3 text-xs text-slate-600 sm:grid-cols-2">
      <div className="rounded-md border border-slate-200 bg-white p-3">
        <div className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">
          {t("processing.legend.functions.title")}
        </div>
        <div className="mt-2 flex flex-wrap gap-2">
          <span className="flex items-center gap-2">
            <span className="h-2.5 w-2.5 rounded-full bg-green-600" />
            {t("processing.legend.completed")}
          </span>
          <span className="flex items-center gap-2">
            <span className="h-2.5 w-2.5 rounded-full bg-blue-600" />
            {t("processing.legend.running")}
          </span>
          <span className="flex items-center gap-2">
            <span className="h-2.5 w-2.5 rounded-full bg-orange-500" />
            {t("processing.legend.failed.default")}
          </span>
          <span className="flex items-center gap-2">
            <span className="h-2.5 w-2.5 rounded-full bg-amber-400" />
            {t("processing.legend.failed.oom")}
          </span>
          <span className="flex items-center gap-2">
            <span className="h-2.5 w-2.5 rounded-full bg-blue-700" />
            {t("processing.legend.failed.concurrent")}
          </span>
          <span className="flex items-center gap-2">
            <span className="h-2.5 w-2.5 rounded-full bg-yellow-300" />
            {t("processing.legend.failed.internal")}
          </span>
          <span className="flex items-center gap-2">
            <span className="h-2.5 w-2.5 rounded-full bg-fuchsia-500" />
            {t("processing.legend.failed.upstream")}
          </span>
          <span className="flex items-center gap-2">
            <span className="h-2.5 w-2.5 rounded-full bg-violet-600" />
            {t("processing.legend.timeout")}
          </span>
        </div>
      </div>
      <div className="rounded-md border border-slate-200 bg-white p-3">
        <div className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">
          {t("processing.legend.transformations.title")}
        </div>
        <div className="mt-2 flex flex-wrap gap-2">
          <span className="flex items-center gap-2">
            <span className="h-2.5 w-2.5 rounded-full bg-green-600" />
            {t("processing.legend.completed")}
          </span>
          <span className="flex items-center gap-2">
            <span className="h-2.5 w-2.5 rounded-full bg-blue-600" />
            {t("processing.legend.running")}
          </span>
          <span className="flex items-center gap-2">
            <span className="h-2.5 w-2.5 rounded-full bg-orange-500" />
            {t("processing.legend.failed")}
          </span>
          <span className="flex items-center gap-2">
            <span className="h-2.5 w-2.5 rounded-full bg-violet-600" />
            {t("processing.legend.timeout")}
          </span>
          <span className="flex items-center gap-2">
            <span className="h-2.5 w-2.5 rounded-full bg-purple-500" />
            {t("processing.legend.other")}
          </span>
        </div>
      </div>
      <div className="rounded-md border border-slate-200 bg-white p-3">
        <div className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">
          {t("processing.legend.workflows.title")}
        </div>
        <div className="mt-2 flex flex-wrap gap-2">
          <span className="flex items-center gap-2">
            <span className="h-2.5 w-2.5 rounded-full bg-green-600" />
            {t("processing.legend.completed")}
          </span>
          <span className="flex items-center gap-2">
            <span className="h-2.5 w-2.5 rounded-full bg-blue-600" />
            {t("processing.legend.running")}
          </span>
          <span className="flex items-center gap-2">
            <span className="h-2.5 w-2.5 rounded-full bg-orange-500" />
            {t("processing.legend.failed")}
          </span>
          <span className="flex items-center gap-2">
            <span className="h-2.5 w-2.5 rounded-full bg-violet-600" />
            {t("processing.legend.timedout")}
          </span>
          <span className="flex items-center gap-2">
            <span className="h-2.5 w-2.5 rounded-full bg-purple-500" />
            {t("processing.legend.other")}
          </span>
        </div>
      </div>
      <div className="rounded-md border border-slate-200 bg-white p-3">
        <div className="text-[11px] font-semibold uppercase tracking-wide text-slate-500">
          {t("processing.legend.extractors.title")}
        </div>
        <div className="mt-2 flex flex-wrap gap-2">
          <span className="flex items-center gap-2">
            <span className="h-2.5 w-2.5 rounded-full bg-cyan-500" />
            {t("processing.legend.seen")}
          </span>
          <span className="flex items-center gap-2">
            <span className="h-2.5 w-2.5 rounded-full bg-green-600" />
            {t("processing.legend.success")}
          </span>
          <span className="flex items-center gap-2">
            <span className="h-2.5 w-2.5 rounded-full bg-orange-500" />
            {t("processing.legend.failed")}
          </span>
          <span className="flex items-center gap-2">
            <span className="h-2.5 w-2.5 rounded-full bg-blue-600" />
            {t("processing.legend.running")}
          </span>
          <span className="flex items-center gap-2">
            <span className="h-2.5 w-2.5 rounded-full bg-purple-500" />
            {t("processing.legend.other")}
          </span>
        </div>
      </div>
    </div>
  );
}
