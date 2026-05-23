import { useMemo } from "react";
import yaml from "js-yaml";
import { useAppSettings } from "../../context/AppSettingsContext";
import { saveGovernanceConfigRaw } from "../../api/governanceDeclared";
import { AdvancedYamlPanel } from "./AdvancedYamlPanel";
import { DimensionsEditor } from "./DimensionsEditor";
import { GovernanceScopePaneHeader } from "./GovernanceConfigPaneHeader";
import { ScopeHierarchyEditor } from "./ScopeHierarchyEditor";
import { useGovernanceDoc } from "./useGovernanceDoc";

export function GovernanceScopePane() {
  const { t } = useAppSettings();
  const gov = useGovernanceDoc();

  const rawYaml = useMemo(
    () => yaml.dump(gov.doc, { lineWidth: -1, noRefs: true }),
    [gov.doc]
  );

  return (
    <div className="disc-gov-pane">
      <GovernanceScopePaneHeader
        dirty={gov.dirty}
        loading={gov.loading}
        saving={gov.saving}
        error={gov.error}
        onReload={() => void gov.load()}
        onSave={() => void gov.save()}
        onMirror={() => void gov.mirror()}
      />
      <div className="disc-gov-pane-body gov-stack gov-stack--lg">
        {gov.loading ? (
          <p className="disc-empty-hint">{t("tree.loading")}</p>
        ) : gov.error ? (
          <p className="disc-banner--error" role="alert">
            {gov.error}
          </p>
        ) : (
          <>
            <ScopeHierarchyEditor doc={gov.doc} onChange={gov.setDoc} />
            <DimensionsEditor doc={gov.doc} onChange={gov.setDoc} />
            <AdvancedYamlPanel
              initialContent={rawYaml}
              onSaveRaw={saveGovernanceConfigRaw}
              onAfterSave={gov.load}
            />
          </>
        )}
      </div>
    </div>
  );
}
