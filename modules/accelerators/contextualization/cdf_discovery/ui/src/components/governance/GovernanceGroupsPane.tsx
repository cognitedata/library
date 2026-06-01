import { useAppSettings } from "../../context/AppSettingsContext";
import { GovernanceConfigPaneHeader } from "./GovernanceConfigPaneHeader";
import { GroupsEditor } from "./GroupsEditor";
import { useGovernanceDoc } from "./useGovernanceDoc";
import type { GovernanceSubTab } from "./GovernanceSpacesPane";

type Props = {
  initialSubTab?: GovernanceSubTab;
  initialArtifactRel?: string | null;
};

export function GovernanceGroupsPane({
  initialSubTab = "configure",
  initialArtifactRel: _initialArtifactRel,
}: Props) {
  const { t } = useAppSettings();
  const gov = useGovernanceDoc();
  const subTab = initialSubTab;

  return (
    <div className="disc-gov-pane">
      <GovernanceConfigPaneHeader
        subTab={subTab}
        onSubTabChange={() => undefined}
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
        ) : (
          <GroupsEditor doc={gov.doc} onChange={gov.setDoc} />
        )}
      </div>
    </div>
  );
}
