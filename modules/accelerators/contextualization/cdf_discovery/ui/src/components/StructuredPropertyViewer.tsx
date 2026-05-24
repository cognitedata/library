import { useCallback, useState } from "react";
import { useAppSettings } from "../context/AppSettingsContext";
import { formatGridCell } from "../utils/gridFormat";
import {
  childCount,
  childPropertyEntries,
  normalizePropertyPayload,
  propertyValueKind,
} from "../utils/propertyTree";

type Props = {
  value: unknown;
  preferredKeys?: string[];
  compact?: boolean;
};

const MAX_DEPTH = 12;

function PropertyRow({
  name,
  path,
  value,
  depth,
  preferredKeys,
  compact,
  expandedPaths,
  onToggle,
}: {
  name: string;
  path: string;
  value: unknown;
  depth: number;
  preferredKeys?: string[];
  compact?: boolean;
  expandedPaths: Set<string>;
  onToggle: (path: string) => void;
}) {
  const { t } = useAppSettings();
  const kind = propertyValueKind(value);
  const expandable = kind === "object" || kind === "array";
  const expanded = expandedPaths.has(path);
  const count = expandable ? childCount(value) : 0;

  const displayValue =
    kind === "null" || (kind === "primitive" && formatGridCell(value) === "")
      ? t("common.emptyValue")
      : kind === "primitive"
        ? formatGridCell(value)
        : t("properties.childCount", { count: String(count) });

  return (
    <>
      <tr className={`disc-prop-row disc-prop-row--depth-${Math.min(depth, 6)}`}>
        <td className="disc-prop-row__name">
          <span className="disc-prop-row__indent" style={{ paddingLeft: `${depth * 0.65}rem` }}>
            {expandable ? (
              <button
                type="button"
                className="disc-prop-row__expander"
                aria-expanded={expanded}
                aria-label={name}
                onClick={() => onToggle(path)}
              >
                {expanded ? "▾" : "▸"}
              </button>
            ) : (
              <span className="disc-prop-row__expander disc-prop-row__expander--spacer" aria-hidden />
            )}
            <span className="disc-prop-row__label">{name}</span>
          </span>
        </td>
        <td
          className={`disc-prop-row__value${kind === "primitive" ? " disc-prop-row__value--mono" : ""}`}
          onClick={expandable ? () => onToggle(path) : undefined}
          role={expandable ? "button" : undefined}
          tabIndex={expandable ? 0 : undefined}
          onKeyDown={
            expandable
              ? (e) => {
                  if (e.key === "Enter" || e.key === " ") {
                    e.preventDefault();
                    onToggle(path);
                  }
                }
              : undefined
          }
        >
          {displayValue}
        </td>
      </tr>
      {expandable &&
        expanded &&
        depth < MAX_DEPTH &&
        childPropertyEntries(value, path, preferredKeys).map((child) => (
          <PropertyRow
            key={child.path}
            name={child.key}
            path={child.path}
            value={child.value}
            depth={depth + 1}
            preferredKeys={preferredKeys}
            compact={compact}
            expandedPaths={expandedPaths}
            onToggle={onToggle}
          />
        ))}
    </>
  );
}

export function StructuredPropertyViewer({ value, preferredKeys, compact = false }: Props) {
  const { t } = useAppSettings();
  const [expandedPaths, setExpandedPaths] = useState<Set<string>>(() => new Set());

  const onToggle = useCallback((path: string) => {
    setExpandedPaths((prev) => {
      const next = new Set(prev);
      if (next.has(path)) next.delete(path);
      else next.add(path);
      return next;
    });
  }, []);

  const payload = normalizePropertyPayload(value, preferredKeys);
  const entries = childPropertyEntries(payload, "", preferredKeys);

  if (entries.length === 0) {
    return <p className="disc-empty-hint">{t("properties.emptyHint")}</p>;
  }

  return (
    <div className={`disc-prop-viewer${compact ? " disc-prop-viewer--compact" : ""}`}>
      <table className="disc-prop-table">
        <thead>
          <tr>
            <th scope="col">{t("properties.columnName")}</th>
            <th scope="col">{t("properties.columnValue")}</th>
          </tr>
        </thead>
        <tbody>
          {entries.map((entry) => (
            <PropertyRow
              key={entry.path}
              name={entry.key}
              path={entry.path}
              value={entry.value}
              depth={0}
              preferredKeys={preferredKeys}
              compact={compact}
              expandedPaths={expandedPaths}
              onToggle={onToggle}
            />
          ))}
        </tbody>
      </table>
    </div>
  );
}
