import { getAppConfig } from "@/config/appConfig";
import type { ViewId } from "@/types/operations";

/** CDF view identifiers from the loaded config node. */
export const VIEW_IDS: {
  readonly SUMMARY_OUTPUT_FILE: ViewId;
  readonly FILE: ViewId;
  readonly ASSET: ViewId;
  readonly TIME_SERIES: ViewId;
  readonly ACTIVITY: ViewId;
  readonly EQUIPMENT: ViewId;
} = {
  get SUMMARY_OUTPUT_FILE() {
    return getAppConfig().dataModel.views.summaryOutputFile;
  },
  get FILE() {
    return getAppConfig().dataModel.views.file;
  },
  get ASSET() {
    return getAppConfig().dataModel.views.asset;
  },
  get TIME_SERIES() {
    return getAppConfig().dataModel.views.timeSeries;
  },
  get ACTIVITY() {
    return getAppConfig().dataModel.views.activity;
  },
  get EQUIPMENT() {
    return getAppConfig().dataModel.views.equipment;
  },
};
