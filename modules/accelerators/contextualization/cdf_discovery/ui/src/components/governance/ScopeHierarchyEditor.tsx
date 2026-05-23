import { useAppSettings } from "../../context/AppSettingsContext";
import type { GovernanceDocument, HierarchyDimension } from "../../types/governanceConfig";
import { DimensionsHierarchyEditor } from "./DimensionsHierarchyEditor";

type Props = {
  doc: GovernanceDocument;
  onChange: (next: GovernanceDocument) => void;
};

const DEFAULT_SCOPE: HierarchyDimension = {
  type: "hierarchy",
  levels: ["site", "unit", "area", "system"],
  locations: [],
};

export function ScopeHierarchyEditor({ doc, onChange }: Props) {
  const { t } = useAppSettings();
  const block: HierarchyDimension = {
    ...DEFAULT_SCOPE,
    ...(doc.scope_hierarchy ?? {}),
    type: "hierarchy",
  };

  return (
    <section className="gov-section">
      <h2 className="gov-section-title">{t("dimensions.scopeHierarchyTitle")}</h2>
      <p className="gov-hint">{t("dimensions.scopeHierarchyHint")}</p>
      <DimensionsHierarchyEditor
        value={block}
        onChange={(scope_hierarchy) => onChange({ ...doc, scope_hierarchy })}
      />
    </section>
  );
}
