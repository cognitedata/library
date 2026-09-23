import { useCallback, useState } from "react";
import type { MessageKey } from "../../i18n";
import { copyTextToClipboard } from "../../utils/clipboardGrid";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  t: TFn;
  runId: string | null | undefined;
  /** When true, show a muted "no run" state instead of hiding. */
  showEmpty?: boolean;
};

/** Pull run id from local-runner / detail log lines when lastRun is not set yet. */
export function extractRunIdFromLog(log: string): string | null {
  const text = String(log || "");
  const fromLocal = text.match(/Local pipeline run_id=([^\s]+)/);
  if (fromLocal?.[1]?.trim()) return fromLocal[1].trim();
  // Prefer the last run_id= occurrence (current run may append after older log text).
  const all = [...text.matchAll(/(?:^|\s)run_id=([A-Za-z0-9_-]+)/g)];
  if (all.length) {
    const last = all[all.length - 1]?.[1]?.trim();
    if (last) return last;
  }
  return null;
}

export function TransformRunIdBanner({ t, runId, showEmpty = true }: Props) {
  const [copied, setCopied] = useState(false);
  const id = String(runId ?? "").trim();

  const onCopy = useCallback(async () => {
    if (!id) return;
    try {
      await copyTextToClipboard(id);
      setCopied(true);
      window.setTimeout(() => setCopied(false), 1500);
    } catch {
      setCopied(false);
    }
  }, [id]);

  if (!id && !showEmpty) return null;

  return (
    <div
      className={`transform-run-id-banner${id ? "" : " transform-run-id-banner--empty"}`}
      role="status"
      aria-label={t("transform.runIdBanner.label")}
    >
      <span className="transform-run-id-banner__label">{t("transform.runIdBanner.label")}</span>
      {id ? (
        <>
          <code className="transform-run-id-banner__value" title={id}>
            {id}
          </code>
          <button
            type="button"
            className="disc-btn disc-btn--ghost transform-run-id-banner__copy"
            onClick={() => void onCopy()}
            aria-label={t("transform.runIdBanner.copy")}
          >
            {copied ? t("transform.runIdBanner.copied") : t("transform.runIdBanner.copy")}
          </button>
        </>
      ) : (
        <span className="transform-run-id-banner__none">{t("transform.runIdBanner.none")}</span>
      )}
    </div>
  );
}
