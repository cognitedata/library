import { useCallback, useEffect, useState } from "react";
import { fetchTransformationDetail, fetchTransformationList } from "../../api";
import { useAppSettings } from "../../context/AppSettingsContext";
import type { TransformationDetail, TransformationListItem } from "../../types/discoveryNodes";

type Props = {
  externalIdValue: string;
  onExternalIdChange: (externalId: string) => void;
  /** When set, Load fetches full detail and calls this (spark import). Omit for link-only ref nodes. */
  onImportDetail?: (detail: TransformationDetail) => void;
  /** When true, confirm before overwriting local external id / SQL via import. */
  confirmBeforeImport?: () => boolean;
};

function listOptionValue(item: TransformationListItem): string {
  if (item.external_id?.trim()) return `ext:${item.external_id.trim()}`;
  return `id:${item.id}`;
}

function resolveListSelection(
  items: TransformationListItem[],
  selectedKey: string
): TransformationListItem | null {
  if (!selectedKey) return null;
  if (selectedKey.startsWith("ext:")) {
    const ext = selectedKey.slice(4);
    return items.find((i) => i.external_id?.trim() === ext) ?? null;
  }
  if (selectedKey.startsWith("id:")) {
    const id = Number(selectedKey.slice(3));
    return items.find((i) => i.id === id) ?? null;
  }
  return null;
}

export function CdfTransformationPicker({
  externalIdValue,
  onExternalIdChange,
  onImportDetail,
  confirmBeforeImport,
}: Props) {
  const { t } = useAppSettings();
  const [items, setItems] = useState<TransformationListItem[]>([]);
  const [listLoading, setListLoading] = useState(false);
  const [listError, setListError] = useState<string | null>(null);
  const [selectedKey, setSelectedKey] = useState("");
  const [loadLoading, setLoadLoading] = useState(false);
  const [loadError, setLoadError] = useState<string | null>(null);

  useEffect(() => {
    let cancelled = false;
    setListLoading(true);
    setListError(null);
    void fetchTransformationList()
      .then((res) => {
        if (cancelled) return;
        setItems(res.items);
        const ext = externalIdValue.trim();
        if (ext) {
          const match = res.items.find((i) => i.external_id?.trim() === ext);
          if (match) setSelectedKey(listOptionValue(match));
        }
      })
      .catch((e) => {
        if (!cancelled) setListError(String(e));
      })
      .finally(() => {
        if (!cancelled) setListLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [externalIdValue]);

  const onLoad = useCallback(async () => {
    const picked = resolveListSelection(items, selectedKey);
    if (!picked) {
      setLoadError(t("transform.spark.pickTransformation"));
      return;
    }
    if (confirmBeforeImport && !confirmBeforeImport()) {
      return;
    }
    setLoadLoading(true);
    setLoadError(null);
    try {
      const detail = await fetchTransformationDetail(
        picked.external_id?.trim()
          ? { externalId: picked.external_id.trim() }
          : { id: picked.id }
      );
      if (onImportDetail) {
        onImportDetail(detail);
      } else {
        const ext = String(detail.external_id ?? "").trim();
        if (ext) onExternalIdChange(ext);
      }
    } catch (e) {
      setLoadError(String(e));
    } finally {
      setLoadLoading(false);
    }
  }, [items, selectedKey, confirmBeforeImport, onExternalIdChange, onImportDetail, t]);

  return (
    <div className="transform-spark-picker">
      <span className="transform-query-label">{t("transform.spark.importFromCdf")}</span>
      {listError ? (
        <p className="transform-query-hint" style={{ color: "var(--disc-error, #b91c1c)" }}>
          {listError}
        </p>
      ) : null}
      <div className="transform-spark-picker__row">
        <select
          className="gov-input transform-spark-picker__select"
          value={selectedKey}
          disabled={listLoading}
          onChange={(e) => setSelectedKey(e.target.value)}
        >
          <option value="">{t("transform.spark.selectTransformation")}</option>
          {items.map((item) => (
            <option key={listOptionValue(item)} value={listOptionValue(item)}>
              {item.label}
            </option>
          ))}
        </select>
        <button
          type="button"
          className="disc-btn disc-btn--sm"
          disabled={listLoading || loadLoading || !selectedKey}
          onClick={() => void onLoad()}
        >
          {loadLoading ? t("transform.spark.loadingTransformation") : t("transform.spark.loadTransformation")}
        </button>
      </div>
      {loadError ? (
        <p className="transform-query-hint" style={{ color: "var(--disc-error, #b91c1c)" }}>
          {loadError}
        </p>
      ) : null}
    </div>
  );
}
