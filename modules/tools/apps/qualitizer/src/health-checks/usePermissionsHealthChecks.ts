import type { CogniteClient } from "@cognite/sdk";
import { useEffect, useState } from "react";
import {
  classifyCompliantGroups,
  computePermissionsHealthOverviewStats,
  computePermissionScopeDrift,
} from "./health-checks-utils";
import type {
  CompliantGroupEntry,
  GroupSummary,
  LoadState,
  PermissionScopeDriftEntry,
  PermissionsHealthOverviewStats,
} from "./types";

type UsePermissionsHealthChecksArgs = {
  sdk: CogniteClient;
  isSdkLoading: boolean;
  enabled?: boolean;
};

export function usePermissionsHealthChecks({
  sdk,
  isSdkLoading,
  enabled = true,
}: UsePermissionsHealthChecksArgs) {
  const [permissionsStatus, setPermissionsStatus] = useState<LoadState>("idle");
  const [permissionsError, setPermissionsError] = useState<string | null>(null);
  const [permissionScopeDrift, setPermissionScopeDrift] = useState<
    PermissionScopeDriftEntry[]
  >([]);
  const [compliantGroups, setCompliantGroups] = useState<CompliantGroupEntry[]>([]);
  const [permissionsStats, setPermissionsStats] =
    useState<PermissionsHealthOverviewStats | null>(null);
  const [checksLoadingPhase, setChecksLoadingPhase] = useState<
    "listing" | "analyzing" | null
  >(null);

  useEffect(() => {
    if (!enabled || isSdkLoading) return;
    let cancelled = false;

    const run = async () => {
      setPermissionsStatus("loading");
      setPermissionsError(null);
      setChecksLoadingPhase("listing");

      try {
        const groups = (await sdk.groups.list({ all: true })) as GroupSummary[];
        if (cancelled) return;

        setChecksLoadingPhase("analyzing");
        const drift = computePermissionScopeDrift(groups);
        const compliant = classifyCompliantGroups(groups, drift);
        const stats = computePermissionsHealthOverviewStats(groups, drift);

        if (!cancelled) {
          setPermissionScopeDrift(drift);
          setCompliantGroups(compliant);
          setPermissionsStats(stats);
          setPermissionsStatus("success");
          setChecksLoadingPhase(null);
        }
      } catch (error) {
        if (!cancelled) {
          setPermissionsError(
            error instanceof Error ? error.message : "Failed to load permissions"
          );
          setPermissionsStatus("error");
          setChecksLoadingPhase(null);
          setPermissionsStats(null);
        }
      }
    };

    run();
    return () => {
      cancelled = true;
    };
  }, [enabled, isSdkLoading, sdk]);

  return {
    permissionsStatus,
    permissionsError,
    permissionScopeDrift,
    compliantGroups,
    permissionsStats,
    checksLoadingPhase,
  };
}
