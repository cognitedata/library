import { useState } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import { GovernanceArtifactsSection } from "./GovernanceArtifactsSection";
import { GovernanceBuildSection } from "./GovernanceBuildSection";
import { GovernanceConfigPaneHeader } from "./GovernanceConfigPaneHeader";
import { SpacesEditor } from "./SpacesEditor";
import { useGovernanceDoc } from "./useGovernanceDoc";

export type GovernanceSubTab = "configure" | "build" | "artifacts";

type Props = {
  initialSubTab?: GovernanceSubTab;
  initialArtifactRel?: string | null;
  onArtifactsChanged?: (workspace: "spaces" | "groups") => void;
};

export function GovernanceSpacesPane({
  initialSubTab = "configure",
  initialArtifactRel,
  onArtifactsChanged,
}: Props) {
  const { t } = useAppSettings();
  const gov = useGovernanceDoc();
  const [subTab, setSubTab] = useState<GovernanceSubTab>(initialSubTab);
  const [artifactRefresh, setArtifactRefresh] = useState(0);

  return (
    <div className="disc-gov-pane">
      <GovernanceConfigPaneHeader
        subTab={subTab}
        onSubTabChange={setSubTab}
        dirty={gov.dirty}
        loading={gov.loading}
        saving={gov.saving}
        error={gov.error}
        onReload={() => void gov.load()}
        onSave={() => void gov.save()}
      />
      <div className="disc-gov-pane-body">
        {gov.loading ? (
          <p className="disc-empty-hint">{t("tree.loading")}</p>
        ) : subTab === "configure" ? (
          <SpacesEditor doc={gov.doc} onChange={gov.setDoc} />
        ) : subTab === "build" ? (
          <GovernanceBuildSection
            target="spaces"
            onBuildComplete={(r) => {
              if (r.ok && !r.dryRun) {
                setArtifactRefresh((n) => n + 1);
                onArtifactsChanged?.("spaces");
                setSubTab("artifacts");
              }
            }}
          />
        ) : (
          <GovernanceArtifactsSection
            kind="spaces"
            doc={gov.doc}
            setDoc={gov.setDoc}
            initialRel={initialArtifactRel}
            refreshToken={artifactRefresh}
          />
        )}
      </div>
    </div>
  );
}
