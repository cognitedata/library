import type { CogniteClient } from "@cognite/sdk";
import { useEffect } from "react";
import { trackDeploymentPackUsageMixpanel } from "@/shared/deploymentPackUsageMixpanel";
import { DEPLOYMENT_PACKS } from "./deployment-packs";
import { detectDeploymentPackUsage } from "./detect";
import { fetchLiveDeploymentPackProbeContext } from "./live-probe-context";

export function useDailyDeploymentPackUsageMixpanel(args: {
  sdk: CogniteClient;
  project: string | undefined;
  enabled: boolean;
}): void {
  const { sdk, project, enabled } = args;

  useEffect(() => {
    if (!enabled || !project?.trim()) return;
    const p = project.trim();
    let cancelled = false;
    void (async () => {
      try {
        const ctx = await fetchLiveDeploymentPackProbeContext(sdk, p);
        if (cancelled) return;
        const results = await detectDeploymentPackUsage(DEPLOYMENT_PACKS, ctx);
        if (cancelled) return;
        trackDeploymentPackUsageMixpanel(p, results, { force: false });
      } catch {
        /* probe failed — skip Mixpanel */
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [sdk, project, enabled]);
}
