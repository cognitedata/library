import { useAppSettings } from "../../context/AppSettingsContext";
import type { MessageKey } from "../../i18n";

export type DataTableColumn<T> = {
  id: string;
  headerKey: MessageKey;
  render: (row: T) => React.ReactNode;
};

type Props<T> = {
  columns: DataTableColumn<T>[];
  rows: T[];
  onRowClick?: (row: T) => void;
  emptyMessage?: string;
};

export function DataTable<T>({ columns, rows, onRowClick, emptyMessage }: Props<T>) {
  const { t } = useAppSettings();

  if (rows.length === 0) {
    return emptyMessage ? <p className="idx-pane__hint">{emptyMessage}</p> : null;
  }

  return (
    <div className="idx-table-wrap">
      <table className="idx-table">
        <thead>
          <tr>
            {columns.map((col) => (
              <th key={col.id}>{t(col.headerKey)}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr
              key={i}
              onClick={onRowClick ? () => onRowClick(row) : undefined}
              style={onRowClick ? { cursor: "pointer" } : undefined}
            >
              {columns.map((col) => (
                <td key={col.id}>{col.render(row)}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
