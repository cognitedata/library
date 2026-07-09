import type { ReactNode } from "react";
import { SettingsPane } from "../../components/settings/SettingsPane";
import type { DiscoveryModule } from "../../shell/discoveryShell";
import type { DocumentTab } from "../../types/discoveryNodes";
import { isSettingsTab } from "../../types/discoveryNodes";

/** Global settings tab (toolbar); not a connection-root tree module. */
export const settingsModule: DiscoveryModule = {
  id: "settings",
  treeRootId: "settings",
  labelKey: "settings.title",

  ownsNode: () => false,
  tryOpenNode: () => false,

  ownsTab(tab: DocumentTab): boolean {
    return isSettingsTab(tab);
  },

  renderTab(tab: DocumentTab): ReactNode | null {
    if (isSettingsTab(tab)) {
      return <SettingsPane />;
    }
    return null;
  },
};
