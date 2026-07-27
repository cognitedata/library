import type { LoadProgress } from "./dms-types";

export function LoadProgressCard({ progress }: { progress: LoadProgress }) {
  const percent =
    progress.total > 0 ? Math.round((100 * progress.current) / progress.total) : progress.current > 0 ? 100 : 30;

  return (
    <div className="rounded-md border border-slate-200 bg-slate-50 p-4">
      <div className="text-sm font-medium text-slate-800">{progress.phase}</div>
      {progress.detail ? <div className="mt-1 text-xs text-slate-600">{progress.detail}</div> : null}
      {progress.total > 0 ? (
        <div className="mt-1 text-xs text-slate-600">
          {progress.current} / {progress.total}
        </div>
      ) : null}
      <div className="mt-2 h-2 w-full overflow-hidden rounded-full bg-slate-200">
        <div
          className="h-full bg-slate-500 transition-all duration-300"
          style={{ width: `${percent}%` }}
        />
      </div>
    </div>
  );
}
