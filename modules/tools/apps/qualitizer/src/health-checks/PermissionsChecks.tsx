import { useEffect, useMemo, useState } from "react";
import { Loader } from "@/shared/Loader";
import { useAppSdk } from "@/shared/auth";
import {
  computePermissionScopeDrift,
  classifyCompliantGroups,
} from "./health-checks-utils";
import { PermissionsHealthPanel } from "./PermissionsHealthPanel";
import type { GroupSummary, LoadState } from "./types";

type Props = { onBack: () => void };

export function PermissionsChecks({ onBack }: Props) {
  const { sdk, isLoading: isSdkLoading } = useAppSdk();

  const [permissionsStatus, setPermissionsStatus] = useState<LoadState>("idle");
  const [permissionsError, setPermissionsError] = useState<string | null>(null);
  const [groups, setGroups] = useState<GroupSummary[]>([]);
  const [showLoader, setShowLoader] = useState(false);

  useEffect(() => {
    setShowLoader(permissionsStatus === "loading");
  }, [permissionsStatus]);

  useEffect(() => {
    if (isSdkLoading) return;
    let cancelled = false;
    const loadPermissions = async () => {
      setPermissionsStatus("loading");
      setPermissionsError(null);
      try {
        const groupResponse = (await sdk.groups.list({
          all: true,
        })) as GroupSummary[];
        if (!cancelled) {
          setGroups(groupResponse);
          setPermissionsStatus("success");
        }
      } catch (error) {
        if (!cancelled) {
          setPermissionsError(
            error instanceof Error
              ? error.message
              : "Failed to load permissions"
          );
          setPermissionsStatus("error");
        }
      }
    };
    loadPermissions();
    return () => {
      cancelled = true;
    };
  }, [isSdkLoading, sdk]);

  const permissionScopeDrift = useMemo(
    () => computePermissionScopeDrift(groups),
    [groups]
  );

  const compliantGroups = useMemo(
    () => classifyCompliantGroups(groups, permissionScopeDrift),
    [groups, permissionScopeDrift]
  );

  return (
    <section className="flex flex-col gap-4">
      <header className="flex items-start justify-between gap-3">
        <div className="flex flex-col gap-1">
          <h2 className="text-2xl font-semibold text-slate-900">
            Permissions Checks
          </h2>
          <p className="text-sm text-slate-500">
            Permission scope drift between security groups
          </p>
        </div>
        <button
          type="button"
          className="cursor-pointer shrink-0 rounded-md border border-slate-200 bg-white px-3 py-1.5 text-sm text-slate-700 hover:bg-slate-50"
          onClick={onBack}
        >
          Back to checks
        </button>
      </header>
      <PermissionsHealthPanel
        permissionsStatus={permissionsStatus}
        permissionsError={permissionsError}
        permissionScopeDrift={permissionScopeDrift}
        compliantGroups={compliantGroups}
      />
      <Loader
        open={showLoader}
        onClose={() => setShowLoader(false)}
        title="Running permissions checks…"
      />
    </section>
  );
}
