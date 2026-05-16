import type { DmVersionSnapshot } from "./version-history-types";
import { formatTs, shouldShowUpdatedSeparate } from "./version-history-utils";

export type VersionSnapshotMetaProps = {
  roleLabel: string;
  version: string;
  snap: DmVersionSnapshot;
  fusionUrl: (v: string) => string;
  t: (key: string, params?: Record<string, string | number>) => string;
};

export function VersionSnapshotMeta({ roleLabel, version, snap, fusionUrl, t }: VersionSnapshotMetaProps) {
  const ct = formatTs(snap.createdTime);
  const lut = formatTs(snap.lastUpdatedTime);
  return (
    <div className="flex flex-wrap items-baseline gap-x-2 gap-y-0.5 text-[11px] leading-snug">
      <span className="shrink-0 font-medium text-slate-600">{roleLabel}</span>
      <a
        href={fusionUrl(version)}
        target="_blank"
        rel="noreferrer"
        className="font-mono font-semibold text-blue-600 hover:underline"
      >
        {version}
      </a>
      {ct ? (
        <span className="text-slate-500">
          {t("dataCatalog.versionHistory.created")}: {ct}
        </span>
      ) : null}
      {lut && shouldShowUpdatedSeparate(snap.createdTime, snap.lastUpdatedTime) ? (
        <span className="text-slate-500">{t("dataCatalog.versionHistory.updated")}: {lut}</span>
      ) : null}
    </div>
  );
}
