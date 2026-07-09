import type { DiscoveryModule } from "../shell/discoveryShell";
import { dataModule } from "./data/module";
import { extractModule } from "./extract/module";
import { fusionModule } from "./fusion/module";
import { governanceModule } from "./governance/module";
import { invertedIndexModule } from "./invertedIndex/module";
import { monitorModule } from "./monitor/module";
import { settingsModule } from "./settings/module";
import { transformModule } from "./transform/module";

/** Registered discovery modules in connection-root tree order. */
export const DISCOVERY_MODULES: DiscoveryModule[] = [
  dataModule,
  fusionModule,
  governanceModule,
  extractModule,
  transformModule,
  invertedIndexModule,
  monitorModule,
  settingsModule,
];
