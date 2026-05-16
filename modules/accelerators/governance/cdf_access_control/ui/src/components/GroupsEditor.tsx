import { useAppSettings } from "../context/AppSettingsContext";
import type { GovernanceDocument, GroupsConfig, OrgWideGroup } from "../types/governanceConfig";
import { hierarchyDimensionKeys, listDimensionKeys } from "../types/governanceConfig";
import { DeferredCommitInput, DeferredCommitTextarea } from "./DeferredCommitTextField";
import { SourceIdsTable } from "./SourceIdsTable";

type Props = {
  doc: GovernanceDocument;
  onChange: (next: GovernanceDocument) => void;
};

function orgWide(groups: GroupsConfig): OrgWideGroup {
  return { ...(groups.org_wide ?? groups.orgWide ?? {}) };
}

export function GroupsEditor({ doc, onChange }: Props) {
  const { t } = useAppSettings();
  const groups: GroupsConfig = { ...(doc.groups ?? {}) };
  const global = { source_ids: {}, sourceId: "", ...(groups.global ?? {}) };
  const ow = orgWide(groups);
  const hierKeys = hierarchyDimensionKeys(doc.dimensions);
  const listKeys = listDimensionKeys(doc.dimensions);
  const combine = new Set(groups.combine_with ?? []);

  const patch = (patchGroups: Partial<GroupsConfig>) => {
    onChange({ ...doc, groups: { ...groups, ...patchGroups } });
  };

  const patchGlobal = (patchG: Partial<typeof global>) => {
    patch({ global: { ...global, ...patchG } });
  };

  const patchOrgWide = (patchOw: Partial<OrgWideGroup>) => {
    patch({ org_wide: { ...ow, ...patchOw } });
  };

  const extraSpacesText = (): string => {
    const raw = ow.extra_instance_spaces ?? ow.extraInstanceSpaces;
    if (Array.isArray(raw)) return raw.join("\n");
    return typeof raw === "string" ? raw : "";
  };

  const setExtraSpaces = (text: string) => {
    const lines = text
      .split("\n")
      .map((s) => s.trim())
      .filter(Boolean);
    patchOrgWide({ extra_instance_spaces: lines });
  };

  return (
    <div className="gov-stack">
      <section className="gov-section">
        <h2 className="gov-section-title" title={t("groups.section.expansion.tooltip")}>
          {t("groups.section.expansion")}
        </h2>
        <div className="gov-grid-2">
          <label className="gov-label" title={t("groups.field.scopeDimension.tooltip")}>
            {t("groups.field.scopeDimension")}
            <select
              className="gov-input"
              value={groups.scope_dimension ?? ""}
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
              committedValue={groups.name_template ?? ""}
              onCommit={(name_template) => patch({ name_template })}
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
                {k}
              </label>
            ))}
          </fieldset>
        )}
      </section>

      <section className="gov-section">
        <h2 className="gov-section-title" title={t("groups.section.global.tooltip")}>
          {t("groups.section.global")}
        </h2>
        <label className="gov-label" title={t("groups.field.sourceId.tooltip")}>
          {t("groups.field.sourceId")}
          <DeferredCommitInput
            className="gov-input"
            committedValue={global.sourceId ?? ""}
            onCommit={(sourceId) => patchGlobal({ sourceId })}
          />
        </label>
        <p className="gov-hint">{t("groups.hint.sourceIds")}</p>
        <h3 className="gov-subsection-title" title={t("groups.field.sourceIds.tooltip")}>
          {t("groups.field.sourceIds")}
        </h3>
        <SourceIdsTable
          value={global.source_ids ?? {}}
          onChange={(source_ids) => patchGlobal({ source_ids })}
          nameColumnKey="groups.table.cdfGroupName"
          valueColumnKey="groups.table.entraObjectId"
          addRowKey="groups.addSourceIdRow"
        />
      </section>

      <section className="gov-section">
        <h2 className="gov-section-title" title={t("groups.field.orgWide.tooltip")}>
          {t("groups.field.orgWide")}
        </h2>
        <label className="gov-check" title={t("groups.field.enabled.tooltip")}>
          <input
            type="checkbox"
            checked={Boolean(ow.enabled)}
            onChange={(e) => patchOrgWide({ enabled: e.target.checked })}
          />
          {t("groups.field.enabled")}
        </label>
        <div className="gov-grid-2">
          <label className="gov-label" title={t("groups.field.name.tooltip")}>
            {t("groups.field.name")}
            <DeferredCommitInput
              className="gov-input"
              committedValue={ow.name ?? ""}
              onCommit={(name) => patchOrgWide({ name })}
            />
          </label>
          <label className="gov-label" title={t("groups.field.scopeId.tooltip")}>
            {t("groups.field.scopeId")}
            <DeferredCommitInput
              className="gov-input"
              committedValue={String(ow.scope_id ?? ow.scopeId ?? "")}
              onCommit={(scope_id) => patchOrgWide({ scope_id })}
            />
          </label>
          <label className="gov-label gov-label--full" title={t("groups.field.description.tooltip")}>
            {t("groups.field.description")}
            <DeferredCommitTextarea
              className="gov-textarea"
              rows={2}
              committedValue={ow.description ?? ""}
              onCommit={(description) => patchOrgWide({ description })}
            />
          </label>
          <label className="gov-label gov-label--full" title={t("groups.field.extraInstanceSpaces.tooltip")}>
            {t("groups.field.extraInstanceSpaces")}
            <DeferredCommitTextarea
              className="gov-textarea"
              rows={4}
              committedValue={extraSpacesText()}
              onCommit={setExtraSpaces}
            />
          </label>
        </div>
      </section>
    </div>
  );
}
