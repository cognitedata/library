import { useEffect, useRef } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";

type Props = {
  log: string;
  loading: boolean;
};

export function OperationConsole({ log, loading }: Props) {
  const { t } = useAppSettings();
  const logRef = useRef<HTMLTextAreaElement>(null);

  useEffect(() => {
    const el = logRef.current;
    if (!el) return;
    el.scrollTop = el.scrollHeight;
  }, [log, loading]);

  const value = loading && !log ? t("ops.console.running") : log;

  return (
    <section className="idx-operation-console" aria-live="polite">
      <div className="idx-operation-console__header">
        <h3 className="idx-operation-console__title">{t("ops.console.title")}</h3>
      </div>
      <p className="idx-pane__hint">{t("ops.console.hint")}</p>
      <textarea
        ref={logRef}
        readOnly
        className="idx-operation-console__log"
        value={value}
        placeholder={t("ops.console.placeholder")}
        aria-label={t("ops.console.title")}
      />
    </section>
  );
}
