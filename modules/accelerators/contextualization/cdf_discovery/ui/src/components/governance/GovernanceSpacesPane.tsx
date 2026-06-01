import { useAppSettings } from "../../context/AppSettingsContext";
import { GovernanceConfigPaneHeader } from "./GovernanceConfigPaneHeader";
import { SpacesEditor } from "./SpacesEditor";
import { useGovernanceDoc } from "./useGovernanceDoc";

export type GovernanceSubTab = "configure";

type Props = {
  initialSubTab?: GovernanceSubTab;
  initialArtifactRel?: string | null;
};

export function GovernanceSpacesPane({
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
          <SpacesEditor doc={gov.doc} onChange={gov.setDoc} />
        )}
      </div>
    </div>
  );
}
