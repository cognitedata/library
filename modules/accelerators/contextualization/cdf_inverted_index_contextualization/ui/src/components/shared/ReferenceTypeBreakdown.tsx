import { useAppSettings } from "../../context/AppSettingsContext";

type Props = {
  data: Record<string, unknown> | null | undefined;
};

export function ReferenceTypeBreakdown({ data }: Props) {
  const { t } = useAppSettings();
  const byType = data?.references_found_by_type;
  if (!byType || typeof byType !== "object" || Array.isArray(byType)) return null;

  const entries = Object.entries(byType as Record<string, number>).sort(
    (a, b) => b[1] - a[1] || a[0].localeCompare(b[0]),
  );
  if (entries.length === 0) return null;

  return (
    <div className="idx-reference-breakdown">
      <h4 className="idx-reference-breakdown__title">{t("targetDriven.referencesByType")}</h4>
      <div className="idx-stat-grid">
        {entries.map(([referenceType, count]) => (
          <div key={referenceType} className="idx-stat-card">
            <div className="idx-stat-card__label">{referenceType}</div>
            <div className="idx-stat-card__value">{count}</div>
          </div>
        ))}
      </div>
    </div>
  );
}
