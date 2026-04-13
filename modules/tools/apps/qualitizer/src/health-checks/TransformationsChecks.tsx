import { useEffect, useState } from "react";
import { Loader } from "@/shared/Loader";
import { useAppSdk } from "@/shared/auth";
import { TransformationsHealthPanel } from "./TransformationsHealthPanel";
import { useTransformationsHealthChecks } from "./useTransformationsHealthChecks";

type Props = { onBack: () => void };

export function TransformationsChecks({ onBack }: Props) {
  const { sdk, isLoading: isSdkLoading } = useAppSdk();

  const {
    dmvStatus,
    dmvError,
    dmvInconsistencies,
    noopStatus,
    noopError,
    noopTransformations,
    noopTotal,
    transformationsSampleMode,
    onLoadAllTransformations,
    checksLoadingPhase,
    noopCheckProgress,
  } = useTransformationsHealthChecks({ sdk, isSdkLoading });

  const [showLoader, setShowLoader] = useState(false);

  const isDashboardLoading = dmvStatus === "loading" || noopStatus === "loading";

  useEffect(() => {
    setShowLoader(isDashboardLoading);
  }, [isDashboardLoading]);

  return (
    <section className="flex flex-col gap-4">
      <header className="flex items-start justify-between gap-3">
        <div className="flex flex-col gap-1">
          <h2 className="text-2xl font-semibold text-slate-900">Transformations Checks</h2>
          <p className="text-sm text-slate-500">
            Write efficiency and data model version consistency
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

      <TransformationsHealthPanel
        noopStatus={noopStatus}
        noopError={noopError}
        noopTransformations={noopTransformations}
        noopTotal={noopTotal}
        dmvStatus={dmvStatus}
        dmvError={dmvError}
        dmvInconsistencies={dmvInconsistencies}
        checksLoadingPhase={checksLoadingPhase}
        noopCheckProgress={noopCheckProgress}
        transformationsSampleMode={transformationsSampleMode}
        onLoadAllTransformations={onLoadAllTransformations}
      />
      <Loader
        open={showLoader}
        onClose={() => setShowLoader(false)}
        title="Running transformations checks…"
      />
    </section>
  );
}
