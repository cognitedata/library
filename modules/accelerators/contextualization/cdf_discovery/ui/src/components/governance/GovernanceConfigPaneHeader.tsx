import { useAppSettings } from "../../context/AppSettingsContext";
import type { GovernanceSubTab } from "./GovernanceSpacesPane";
import { GovernanceToolbar } from "./GovernanceToolbar";

type ToolbarProps = {
  dirty: boolean;
  loading: boolean;
  saving: boolean;
  error: string | null;
  onReload: () => void;
  onSave: () => void;
  onMirror?: () => void;
};

type Props = ToolbarProps & {
  subTab: GovernanceSubTab;
  onSubTabChange: (tab: GovernanceSubTab) => void;
};

/** Configure / Build / Artifacts subtabs plus save toolbar on one row (spaces & groups panes). */
export function GovernanceConfigPaneHeader({ subTab, onSubTabChange, ...toolbar }: Props) {
  const { t } = useAppSettings();
  return (
    <header className="disc-gov-pane-header">
      <div className="disc-gov-pane-header__row">
        <div className="disc-gov-subtabs disc-gov-subtabs--in-header" role="tablist">
          {(["configure", "build", "artifacts"] as const).map((key) => (
            <button
              key={key}
              type="button"
              role="tab"
              aria-selected={subTab === key}
              className={`disc-gov-subtab${subTab === key ? " disc-gov-subtab--active" : ""}`}
              onClick={() => onSubTabChange(key)}
            >
              {t(`governance.subtab.${key}`)}
            </button>
          ))}
        </div>
        <GovernanceToolbar {...toolbar} />
      </div>
    </header>
  );
}

/** Save / reload toolbar only (scope governance pane). */
export function GovernanceScopePaneHeader(props: ToolbarProps) {
  return (
    <header className="disc-gov-pane-header">
      <div className="disc-gov-pane-header__row disc-gov-pane-header__row--end">
        <GovernanceToolbar {...props} />
      </div>
    </header>
  );
}
