import { useAppSettings } from "../../context/AppSettingsContext";
import type { GovernanceDocument, GroupsConfig } from "../../types/governanceConfig";
import {
  DEFAULT_GROUP_DISPLAY_NAME_TEMPLATE,
  DEFAULT_GROUP_NAME_TEMPLATE,
  listDimensionKeys,
} from "../../types/governanceConfig";
import { DeferredCommitInput } from "./DeferredCommitTextField";
import { governanceDimensionLabel } from "../../utils/governanceDimensionLabel";

type Props = {
  doc: GovernanceDocument;
  onChange: (next: GovernanceDocument) => void;
};

export function GroupsEditor({ doc, onChange }: Props) {
  const { t } = useAppSettings();
  const groups: GroupsConfig = { ...(doc.groups ?? {}) };
  const listKeys = listDimensionKeys(doc.dimensions);
  const combine = new Set(groups.combine_with ?? []);

  const patch = (patchGroups: Partial<GroupsConfig>) => {
    onChange({ ...doc, groups: { ...groups, ...patchGroups } });
  };

  return (
    <div className="gov-stack">
      <section className="gov-section">
        <h2 className="gov-section-title" title={t("groups.section.expansion.tooltip")}>
          {t("groups.section.expansion")}
        </h2>
        <div className="gov-grid-2">
          <label className="gov-label" title={t("groups.field.nodes.tooltip")}>
            {t("groups.field.nodes")}
            <select
              className="gov-input"
              value={groups.nodes ?? "leaves"}
              onChange={(e) => patch({ nodes: e.target.value as "leaves" | "all" })}
            >
              <option value="leaves">{t("groups.nodes.leaves")}</option>
              <option value="all">{t("groups.nodes.all")}</option>
            </select>
          </label>
          <label className="gov-label" title={t("groups.field.template.tooltip")}>
            {t("groups.field.template")}
            <DeferredCommitInput
              className="gov-input"
              committedValue={groups.template ?? ""}
              onCommit={(template) => patch({ template })}
            />
          </label>
          <label className="gov-label" title={t("groups.field.outputDir.tooltip")}>
            {t("groups.field.outputDir")}
            <DeferredCommitInput
              className="gov-input"
              committedValue={groups.output_dir ?? "auth"}
              onCommit={(output_dir) => patch({ output_dir })}
            />
          </label>
          <label className="gov-label" title={t("groups.field.groupNameTemplate.tooltip")}>
            {t("groups.field.groupNameTemplate")}
            <DeferredCommitInput
              className="gov-input"
              committedValue={groups.name_template ?? DEFAULT_GROUP_NAME_TEMPLATE}
              placeholder={t("groups.placeholder.groupNameTemplate")}
              onCommit={(name_template) => patch({ name_template })}
            />
          </label>
          <label className="gov-label" title={t("groups.field.displayNameTemplate.tooltip")}>
            {t("groups.field.displayNameTemplate")}
            <DeferredCommitInput
              className="gov-input"
              committedValue={groups.display_name_template ?? DEFAULT_GROUP_DISPLAY_NAME_TEMPLATE}
              placeholder={t("groups.placeholder.displayNameTemplate")}
              onCommit={(display_name_template) => patch({ display_name_template })}
            />
          </label>
        </div>
        {listKeys.length > 0 && (
          <fieldset className="gov-fieldset">
            <legend title={t("groups.field.combineWith.tooltip")}>{t("groups.field.combineWith")}</legend>
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
