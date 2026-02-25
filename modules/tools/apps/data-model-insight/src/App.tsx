import { useState } from "react";
import { useDune } from "@cognite/dune";
import { useDataModels } from "@/hooks/useDataModels";
import { useDataModelDoc } from "@/hooks/useDataModelDoc";
import type { DataModelSelection } from "@/hooks/useDataModelDoc";
import { ModelSelector } from "@/components/ModelSelector";
import { DocView } from "@/components/DocView";

function App() {
  const { sdk, isLoading } = useDune();
  const [selection, setSelection] = useState<DataModelSelection | null>(null);

  const { data: models = [], isLoading: modelsLoading, error: modelsError } = useDataModels(sdk, undefined);
  const { data: doc, isLoading: docLoading, error: docError } = useDataModelDoc(sdk, selection);

  return (
    <div className="dm-doc-app min-h-screen w-full">
      {isLoading && (
        <div className="min-h-screen flex items-center justify-center bg-slate-950 text-slate-400">
          Loading…
        </div>
      )}
      {!isLoading && !sdk && (
        <div className="min-h-screen flex items-center justify-center bg-slate-950 text-slate-400">
          Not authenticated. Open this app from Dune / Cognite Fusion.
        </div>
      )}
      {!isLoading && sdk && selection && doc && (
        <DocView doc={doc} onBack={() => setSelection(null)} />
      )}
      {!isLoading && sdk && selection && docLoading && (
        <div className="min-h-screen flex items-center justify-center bg-slate-950 text-slate-400">
          Loading documentation…
        </div>
      )}
      {!isLoading && sdk && selection && docError && (
        <div className="min-h-screen flex flex-col items-center justify-center bg-slate-950 text-slate-100 p-6 gap-4">
          <p className="text-red-400">Failed to load model: {(docError as Error).message}</p>
          <button
            type="button"
            onClick={() => setSelection(null)}
            className="dm-btn-primary rounded-lg bg-amber-600 text-slate-900 px-4 py-2 font-medium hover:bg-amber-500"
          >
            Back to selector
          </button>
        </div>
      )}
      {!isLoading && sdk && !(selection && (doc || docLoading || docError)) && (
        <ModelSelector
          models={models}
          isLoading={modelsLoading}
          error={modelsError ?? null}
          onSelect={setSelection}
        />
      )}
    </div>
  );
}

export default App;
