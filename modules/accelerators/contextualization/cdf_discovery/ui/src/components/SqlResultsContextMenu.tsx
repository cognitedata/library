import { useEffect } from "react";
import { createPortal } from "react-dom";
import type { MessageKey } from "../i18n";
import { QUERY_EXPORT_FORMATS, type QueryExportFormat } from "../utils/exportQueryResults";

export type SqlResultsMenuTarget =
  | { kind: "grid" }
  | { kind: "column"; column: string }
  | { kind: "row"; row: Record<string, unknown> }
  | { kind: "cell"; row: Record<string, unknown>; column: string };

export type SqlResultsContextMenuState = {
  x: number;
  y: number;
  target: SqlResultsMenuTarget;
};

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  menu: SqlResultsContextMenuState | null;
  onClose: () => void;
  t: TFn;
  exportFormatLabel: Record<QueryExportFormat, string>;
  queryFileLabel: string;
  canQueryFile: boolean;
  canDownloadFile: boolean;
  downloading: boolean;
  hasResults: boolean;
  onCopyRow: () => void;
  onCopyCell: () => void;
  onCopyResults: () => void;
  onCopyColumn: () => void;
  onSortAsc: () => void;
  onSortDesc: () => void;
  onClearSort: () => void;
  onExport: (format: QueryExportFormat) => void;
  onQueryFile: () => void;
  onDownloadFile: () => void;
  exporting: boolean;
};

function MenuItem({
  label,
  disabled,
  onClick,
}: {
  label: string;
  disabled?: boolean;
  onClick: () => void;
}) {
  return (
    <li>
      <button type="button" disabled={disabled} onClick={onClick}>
        {label}
      </button>
    </li>
  );
}

function MenuSeparator() {
  return <li className="disc-ctx-menu__sep" aria-hidden />;
}

export function SqlResultsContextMenu({
  menu,
  onClose,
  t,
  exportFormatLabel,
  queryFileLabel,
  canQueryFile,
  canDownloadFile,
  downloading,
  hasResults,
  onCopyRow,
  onCopyCell,
  onCopyResults,
  onCopyColumn,
  onSortAsc,
  onSortDesc,
  onClearSort,
  onExport,
  onQueryFile,
  onDownloadFile,
  exporting,
}: Props) {
  useEffect(() => {
    if (!menu) return;
    const close = () => onClose();
    window.addEventListener("click", close);
    window.addEventListener("scroll", close, true);
    return () => {
      window.removeEventListener("click", close);
      window.removeEventListener("scroll", close, true);
    };
  }, [menu, onClose]);

  if (!menu) return null;

  const target = menu.target;
  const showRowActions = target.kind === "row" || target.kind === "cell";
  const showCellAction = target.kind === "cell";
  const showColumnActions = target.kind === "column";
  const showGridActions = target.kind === "grid" || showRowActions;

  return createPortal(
    <ul
      className="disc-ctx-menu"
      style={{ left: menu.x, top: menu.y }}
      onClick={(e) => e.stopPropagation()}
      onContextMenu={(e) => e.preventDefault()}
    >
      {showCellAction && (
        <MenuItem
          label={t("sql.copyCell")}
          onClick={() => {
            onCopyCell();
            onClose();
          }}
        />
      )}
      {showRowActions && (
        <MenuItem
          label={t("sql.copyRow")}
          onClick={() => {
            onCopyRow();
            onClose();
          }}
        />
      )}
      {showRowActions && canQueryFile && (
        <MenuItem
          label={queryFileLabel}
          onClick={() => {
            onQueryFile();
            onClose();
          }}
        />
      )}
      {showRowActions && canDownloadFile && (
        <MenuItem
          label={downloading ? t("sql.downloadFileInProgress") : t("sql.downloadFile")}
          disabled={downloading}
          onClick={() => {
            onDownloadFile();
            onClose();
          }}
        />
      )}
      {showColumnActions && (
        <>
          <MenuItem
            label={t("sql.copyColumn")}
            onClick={() => {
              onCopyColumn();
              onClose();
            }}
          />
          <MenuItem
            label={t("sql.sortAsc")}
            onClick={() => {
              onSortAsc();
              onClose();
            }}
          />
          <MenuItem
            label={t("sql.sortDesc")}
            onClick={() => {
              onSortDesc();
              onClose();
            }}
          />
          <MenuItem
            label={t("sql.clearSort")}
            onClick={() => {
              onClearSort();
              onClose();
            }}
          />
        </>
      )}
      {showGridActions && hasResults && (showRowActions || showColumnActions) && <MenuSeparator />}
      {showGridActions && hasResults && (
        <MenuItem
          label={t("sql.copyResults")}
          onClick={() => {
            onCopyResults();
            onClose();
          }}
        />
      )}
      {showGridActions && hasResults && (
        <>
          <MenuSeparator />
          {QUERY_EXPORT_FORMATS.map((format) => (
            <MenuItem
              key={format}
              label={`${t("sql.export")} ${exportFormatLabel[format]}`}
              disabled={exporting}
              onClick={() => {
                onExport(format);
                onClose();
              }}
            />
          ))}
        </>
      )}
    </ul>,
    document.body
  );
}
