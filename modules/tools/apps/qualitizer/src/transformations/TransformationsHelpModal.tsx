import { useI18n } from "@/shared/i18n";

export type TransformationsHelpSubView = "list" | "overlap" | "dataModelUsage";

type TransformationsHelpModalProps = {
  open: boolean;
  onClose: () => void;
  subView: TransformationsHelpSubView;
};

export function TransformationsHelpModal({ open, onClose, subView }: TransformationsHelpModalProps) {
  const { t } = useI18n();
  if (!open) return null;

  const titleKey =
    subView === "list"
      ? "transformations.help.listTitle"
      : subView === "overlap"
        ? "transformations.help.overlapTitle"
        : "transformations.help.dataModelUsageTitle";
  const contentKey =
    subView === "list"
      ? "transformations.help.list"
      : subView === "overlap"
        ? "transformations.help.overlap"
        : "transformations.help.dataModelUsage";

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/40 p-4"
      onClick={onClose}
    >
      <div
        className="w-full max-w-2xl rounded-lg bg-white p-6 shadow-xl"
        onClick={(event) => event.stopPropagation()}
      >
        <div className="flex items-start justify-between gap-4">
          <h3 className="text-lg font-semibold text-slate-900">{t(titleKey)}</h3>
          <button
            type="button"
            className="rounded-md border border-slate-200 px-3 py-1 text-sm text-slate-700 hover:bg-slate-50"
            onClick={onClose}
          >
            {t("shared.modal.close")}
          </button>
        </div>
        <div className="mt-4 space-y-4 text-sm text-slate-700">
          <p>{t(contentKey)}</p>
          {subView === "list" ? (
            <div className="rounded-md border border-slate-200 bg-slate-50 p-3">
              <div className="mb-2 font-medium text-slate-900">
                {t("transformations.help.columnTableTitle")}
              </div>
              <div className="max-h-[40vh] overflow-auto">
                <table className="w-full border-collapse text-left text-xs">
                  <thead>
                    <tr className="border-b border-slate-200">
                      <th className="py-1.5 pr-3 font-medium text-slate-600">
                        {t("transformations.help.columnTableHeader.col")}
                      </th>
                      <th className="py-1.5 font-medium text-slate-600">
                        {t("transformations.help.columnTableHeader.explanation")}
                      </th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-slate-100">
                    <tr><td className="py-1.5 pr-3 font-medium">{t("transformations.list.name")}</td><td>{t("transformations.list.columnHelp.name")}</td></tr>
                    <tr><td className="py-1.5 pr-3 font-medium">{t("transformations.list.runs24h")}</td><td>{t("transformations.list.columnHelp.runs24h")}</td></tr>
                    <tr><td className="py-1.5 pr-3 font-medium">{t("transformations.list.lastRun")}</td><td>{t("transformations.list.columnHelp.lastRun")}</td></tr>
                    <tr><td className="py-1.5 pr-3 font-medium">{t("transformations.list.totalTime")}</td><td>{t("transformations.list.columnHelp.totalTime")}</td></tr>
                    <tr><td className="py-1.5 pr-3 font-medium">{t("transformations.list.reads")}</td><td>{t("transformations.list.columnHelp.reads")}</td></tr>
                    <tr><td className="py-1.5 pr-3 font-medium">{t("transformations.list.writes")}</td><td>{t("transformations.list.columnHelp.writes")}</td></tr>
                    <tr><td className="py-1.5 pr-3 font-medium">{t("transformations.list.noops")}</td><td>{t("transformations.list.columnHelp.noops")}</td></tr>
                    <tr><td className="py-1.5 pr-3 font-medium">{t("transformations.list.rateLimit429")}</td><td>{t("transformations.list.columnHelp.rateLimit429")}</td></tr>
                    <tr><td className="py-1.5 pr-3 font-medium">Err</td><td>{t("transformations.list.columnHelp.err")}</td></tr>
                    <tr><td className="py-1.5 pr-3 font-medium">Stmt</td><td>{t("transformations.list.columnHelp.stmt")}</td></tr>
                    <tr><td className="py-1.5 pr-3 font-medium">Tok</td><td>{t("transformations.list.columnHelp.tok")}</td></tr>
                    <tr><td className="py-1.5 pr-3 font-medium">Tbl</td><td>{t("transformations.list.columnHelp.tbl")}</td></tr>
                    <tr><td className="py-1.5 pr-3 font-medium">CTE</td><td>{t("transformations.list.columnHelp.cte")}</td></tr>
                    <tr><td className="py-1.5 pr-3 font-medium">DM</td><td>{t("transformations.list.columnHelp.dm")}</td></tr>
                    <tr><td className="py-1.5 pr-3 font-medium">Node</td><td>{t("transformations.list.columnHelp.node")}</td></tr>
                    <tr><td className="py-1.5 pr-3 font-medium">Unit</td><td>{t("transformations.list.columnHelp.unit")}</td></tr>
                    <tr><td className="py-1.5 pr-3 font-medium">Like</td><td>{t("transformations.list.columnHelp.like")}</td></tr>
                    <tr><td className="py-1.5 pr-3 font-medium">Rlike</td><td>{t("transformations.list.columnHelp.rlike")}</td></tr>
                    <tr><td className="py-1.5 pr-3 font-medium">Reg</td><td>{t("transformations.list.columnHelp.reg")}</td></tr>
                    <tr><td className="py-1.5 pr-3 font-medium">Nest</td><td>{t("transformations.list.columnHelp.nest")}</td></tr>
                  </tbody>
                </table>
              </div>
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}
