import { useAppSettings } from "../context/AppSettingsContext";
import type { MessageKey } from "../i18n";

export type ComingSoonWorkspace = "extract" | "monitor";

const TITLE_KEY: Record<ComingSoonWorkspace, MessageKey> = {
  extract: "extract.comingSoon.title",
  monitor: "monitor.comingSoon.title",
};

const MESSAGE_KEY: Record<ComingSoonWorkspace, MessageKey> = {
  extract: "extract.comingSoon.message",
  monitor: "monitor.comingSoon.message",
};

type Props = {
  workspace: ComingSoonWorkspace;
};

export function ComingSoonPane({ workspace }: Props) {
  const { t } = useAppSettings();
  return (
    <div className="disc-coming-soon-pane" role="status">
      <h2 className="disc-coming-soon-pane__title">{t(TITLE_KEY[workspace])}</h2>
      <p className="disc-coming-soon-pane__badge">{t("workspace.comingSoon.badge")}</p>
      <p className="disc-coming-soon-pane__message">{t(MESSAGE_KEY[workspace])}</p>
    </div>
  );
}
