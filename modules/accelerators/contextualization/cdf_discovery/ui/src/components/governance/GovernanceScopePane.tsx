import { useMemo, useState } from "react";
import yaml from "js-yaml";
import { useAppSettings } from "../../context/AppSettingsContext";
import { saveGovernanceConfigRaw } from "../../api/governanceDeclared";
import { AdvancedYamlPanel } from "./AdvancedYamlPanel";
import { DimensionsEditor } from "./DimensionsEditor";
import { GovernanceArtifactsSection } from "./GovernanceArtifactsSection";
import { GovernanceBuildSection } from "./GovernanceBuildSection";
import {
  GovernanceScopePaneHeader,
  type GovernanceScopeSubTab,
} from "./GovernanceConfigPaneHeader";
import { ScopeHierarchyEditor } from "./ScopeHierarchyEditor";
import { useGovernanceDoc } from "./useGovernanceDoc";

export function GovernanceScopePane() {
  const { t } = useAppSettings();
  const gov = useGovernanceDoc();
  const [subTab, setSubTab] = useState<GovernanceScopeSubTab>("scope");

  const rawYaml = useMemo(
    () => yaml.dump(gov.doc, { lineWidth: -1, noRefs: true }),
    [gov.doc]
  );

  return (
    <div className="disc-gov-pane">
      <GovernanceScopePaneHeader
        subTab={subTab}
        onSubTabChange={setSubTab}
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
            {subTab === "scope" ? (
              <ScopeHierarchyEditor doc={gov.doc} onChange={gov.setDoc} />
            ) : subTab === "dimensions" ? (
              <DimensionsEditor doc={gov.doc} onChange={gov.setDoc} />
            ) : subTab === "build" ? (
              <GovernanceBuildSection target="all" />
            ) : (
              <div className="gov-stack gov-stack--lg">
                <GovernanceArtifactsSection kind="spaces" doc={gov.doc} setDoc={gov.setDoc} refreshToken={0} />
                <GovernanceArtifactsSection kind="groups" doc={gov.doc} setDoc={gov.setDoc} refreshToken={0} />
              </div>
            )}
            {subTab === "scope" || subTab === "dimensions" ? (
              <AdvancedYamlPanel
                initialContent={rawYaml}
                onSaveRaw={saveGovernanceConfigRaw}
                onAfterSave={gov.load}
              />
            ) : null}
          </>
        )}
      </div>
    </div>
  );
}
