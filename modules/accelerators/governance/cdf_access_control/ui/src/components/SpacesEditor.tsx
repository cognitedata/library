import { useAppSettings } from "../context/AppSettingsContext";
import type { GovernanceDocument, SpacesConfig } from "../types/governanceConfig";
import { hierarchyDimensionKeys, listDimensionKeys } from "../types/governanceConfig";
import { DeferredCommitInput } from "./DeferredCommitTextField";
import { SourceIdsTable } from "./SourceIdsTable";

type Props = {
  doc: GovernanceDocument;
  onChange: (next: GovernanceDocument) => void;
};

export function SpacesEditor({ doc, onChange }: Props) {
  const { t } = useAppSettings();
  const spaces: SpacesConfig = { ...(doc.spaces ?? {}) };
  const global = { source_ids: {}, sourceId: "", ...(spaces.global ?? {}) };
  const hierKeys = hierarchyDimensionKeys(doc.dimensions);
  const listKeys = listDimensionKeys(doc.dimensions);
  const combine = new Set(spaces.combine_with ?? []);

  const patch = (patchSpaces: Partial<SpacesConfig>) => {
    onChange({ ...doc, spaces: { ...spaces, ...patchSpaces } });
  };

  const patchGlobal = (patchG: Partial<typeof global>) => {
    patch({ global: { ...global, ...patchG } });
  };

  return (
    <div className="gov-stack">
      <section className="gov-section">
        <h2 className="gov-section-title" title={t("spaces.section.expansion.tooltip")}>
          {t("spaces.section.expansion")}
        </h2>
        <p className="gov-hint">
          {t("spaces.hint1")} <code>type: list</code>. {t("spaces.hint2")}
        </p>
        <div className="gov-grid-2">
          <label className="gov-label" title={t("spaces.field.scopeDimension.tooltip")}>
            {t("spaces.field.scopeDimension")}
            <select
              className="gov-input"
              value={spaces.scope_dimension ?? ""}
              onChange={(e) => patch({ scope_dimension: e.target.value })}
            >
              <option value="">—</option>
              {hierKeys.map((k) => (
                <option key={k} value={k}>
                  {k}
                </option>
              ))}
            </select>
          </label>
          <label className="gov-label" title={t("spaces.field.nodes.tooltip")}>
            {t("spaces.field.nodes")}
            <select
              className="gov-input"
              value={spaces.nodes ?? "leaves"}
              onChange={(e) => patch({ nodes: e.target.value as "leaves" | "all" })}
            >
              <option value="leaves">{t("groups.nodes.leaves")}</option>
              <option value="all">{t("groups.nodes.all")}</option>
            </select>
          </label>
          <label className="gov-label" title={t("spaces.field.template.tooltip")}>
            {t("spaces.field.template")}
            <DeferredCommitInput
              className="gov-input"
              committedValue={spaces.template ?? ""}
              onCommit={(template) => patch({ template })}
            />
          </label>
          <label className="gov-label" title={t("spaces.field.outputDir.tooltip")}>
            {t("spaces.field.outputDir")}
            <DeferredCommitInput
              className="gov-input"
              committedValue={spaces.output_dir ?? "spaces"}
              onCommit={(output_dir) => patch({ output_dir })}
            />
          </label>
          <label className="gov-label" title={t("spaces.field.instanceSpaceTemplate.tooltip")}>
            {t("spaces.field.instanceSpaceTemplate")}
            <DeferredCommitInput
              className="gov-input"
              committedValue={spaces.instance_space_id_template ?? ""}
              placeholder={t("spaces.placeholder.instanceSpace")}
              onCommit={(instance_space_id_template) => patch({ instance_space_id_template })}
            />
          </label>
          <label className="gov-label" title={t("spaces.field.nameTemplate.tooltip")}>
            {t("spaces.field.nameTemplate")}
            <DeferredCommitInput
              className="gov-input"
              committedValue={spaces.name_template ?? ""}
              placeholder={t("spaces.placeholder.nameTemplate")}
              onCommit={(name_template) => patch({ name_template })}
            />
          </label>
        </div>
        {listKeys.length > 0 && (
          <fieldset className="gov-fieldset">
            <legend title={t("spaces.field.combineWithList.tooltip")}>
              {t("spaces.field.combineWithList")}
            </legend>
            {listKeys.map((k) => (
              <label key={k} className="gov-check">
                <input
                  type="checkbox"
                  checked={combine.has(k)}
                  onChange={(e) => {
                    const next = new Set(combine);
                    if (e.target.checked) next.add(k);
                    else next.delete(k);
                    patch({ combine_with: [...next] });
                  }}
                />
                {k}
              </label>
            ))}
          </fieldset>
        )}
      </section>

      <section className="gov-section">
        <h2 className="gov-section-title" title={t("spaces.section.global.tooltip")}>
          {t("spaces.section.global")}
        </h2>
        <label className="gov-label" title={t("spaces.field.sourceId.tooltip")}>
          {t("spaces.field.sourceId")}
          <DeferredCommitInput
            className="gov-input"
            committedValue={global.sourceId ?? ""}
            onCommit={(sourceId) => patchGlobal({ sourceId })}
          />
        </label>
        <p className="gov-hint">{t("spaces.hint.sourceIds")}</p>
        <h3 className="gov-subsection-title" title={t("spaces.field.sourceIds.tooltip")}>
          {t("spaces.field.sourceIds")}
        </h3>
        <SourceIdsTable
          value={global.source_ids ?? {}}
          onChange={(source_ids) => patchGlobal({ source_ids })}
          nameColumnKey="spaces.table.instanceSpaceId"
          valueColumnKey="spaces.table.sourceValue"
          addRowKey="spaces.addSourceIdRow"
        />
      </section>
    </div>
  );
}
