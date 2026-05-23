import { useAppSettings } from "../../context/AppSettingsContext";
import type { GovernanceDocument, SpacesConfig } from "../../types/governanceConfig";
import {
  DEFAULT_INSTANCE_SPACE_ID_TEMPLATE,
  DEFAULT_SPACE_NAME_TEMPLATE,
  listDimensionKeys,
} from "../../types/governanceConfig";
import { DeferredCommitInput } from "./DeferredCommitTextField";
import { governanceDimensionLabel } from "../../utils/governanceDimensionLabel";

type Props = {
  doc: GovernanceDocument;
  onChange: (next: GovernanceDocument) => void;
};

export function SpacesEditor({ doc, onChange }: Props) {
  const { t } = useAppSettings();
  const spaces: SpacesConfig = { ...(doc.spaces ?? {}) };
  const listKeys = listDimensionKeys(doc.dimensions);
  const combine = new Set(spaces.combine_with ?? []);

  const patch = (patchSpaces: Partial<SpacesConfig>) => {
    onChange({ ...doc, spaces: { ...spaces, ...patchSpaces } });
  };

  return (
    <div className="gov-stack">
      <section className="gov-section">
        <h2 className="gov-section-title" title={t("spaces.section.expansion.tooltip")}>
          {t("spaces.section.expansion")}
        </h2>
        <p className="gov-hint">
          {t("spaces.hint1")} <code>{t("spaces.hintTypeListYaml")}</code>. {t("spaces.hint2")}
        </p>
        <div className="gov-grid-2">
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
              committedValue={
                spaces.instance_space_id_template ?? DEFAULT_INSTANCE_SPACE_ID_TEMPLATE
              }
              placeholder={t("spaces.placeholder.instanceSpaceTemplate")}
              onCommit={(instance_space_id_template) => patch({ instance_space_id_template })}
            />
          </label>
          <label className="gov-label" title={t("spaces.field.nameTemplate.tooltip")}>
            {t("spaces.field.nameTemplate")}
            <DeferredCommitInput
              className="gov-input"
              committedValue={spaces.name_template ?? DEFAULT_SPACE_NAME_TEMPLATE}
              placeholder={t("spaces.placeholder.nameTemplateDefault")}
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
                {governanceDimensionLabel(k, t)}
              </label>
            ))}
          </fieldset>
        )}
      </section>
    </div>
  );
}
