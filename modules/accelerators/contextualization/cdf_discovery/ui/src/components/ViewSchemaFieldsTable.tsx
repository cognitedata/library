import { useMemo } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import type { ViewSchemaField } from "./query/viewPropertiesApi";

type Props = {
  fields: ViewSchemaField[];
};

function formatFieldType(field: ViewSchemaField): string {
  if (field.kind === "direct_relation" && field.target) {
    return `direct → ${field.target}`;
  }
  if (field.kind === "reverse_direct_relation" && field.target) {
    return `reverse → ${field.target}`;
  }
  if (field.kind === "edge_connection") {
    const parts = [field.connectionType ?? "edge"];
    if (field.target) parts.push(`→ ${field.target}`);
    return parts.join(" ");
  }
  const base = field.type ?? field.kind ?? "unknown";
  if (field.list) return `${base}[]`;
  if (field.nullable === true) return `${base}?`;
  return base;
}

export function ViewSchemaFieldsTable({ fields }: Props) {
  const { t } = useAppSettings();

  const rows = useMemo(
    () => fields.map((field) => ({ field, typeLabel: formatFieldType(field) })),
    [fields]
  );

  return (
    <div className="disc-dm-fields-table-wrap">
      <table className="disc-dm-fields-table">
        <thead>
          <tr>
            <th scope="col">{t("dmViewer.fieldsColumnName")}</th>
            <th scope="col">{t("dmViewer.fieldsColumnType")}</th>
          </tr>
        </thead>
        <tbody>
          {rows.map(({ field, typeLabel }) => (
            <tr key={field.name}>
              <td className="disc-dm-fields-table__name">{field.name}</td>
              <td className="disc-dm-fields-table__type">{typeLabel}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
