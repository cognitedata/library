import { useEffect, useState } from "react";
import { Loader } from "@/shared/Loader";
import { useAppSdk } from "@/shared/auth";
import { PermissionsHealthPanel } from "./PermissionsHealthPanel";
import { usePermissionsHealthChecks } from "./usePermissionsHealthChecks";

type Props = { onBack: () => void };

export function PermissionsChecks({ onBack }: Props) {
  const { sdk, isLoading: isSdkLoading } = useAppSdk();

  const {
    permissionsStatus,
    permissionsError,
    permissionScopeDrift,
    compliantGroups,
    permissionsStats,
    checksLoadingPhase,
  } = usePermissionsHealthChecks({ sdk, isSdkLoading });

  const [showLoader, setShowLoader] = useState(false);

  useEffect(() => {
    setShowLoader(permissionsStatus === "loading");
  }, [permissionsStatus]);

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
        permissionsStats={permissionsStats}
        checksLoadingPhase={checksLoadingPhase}
      />
      <Loader
        open={showLoader}
        onClose={() => setShowLoader(false)}
        title="Running permissions checks…"
      />
    </section>
  );
}
