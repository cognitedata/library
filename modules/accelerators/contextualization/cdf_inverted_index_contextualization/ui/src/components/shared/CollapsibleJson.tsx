import { useState } from "react";
import { redactForDisplay } from "../../api";
import { useAppSettings } from "../../context/AppSettingsContext";

type Props = {
  data: unknown;
  defaultOpen?: boolean;
};

export function CollapsibleJson({ data, defaultOpen = false }: Props) {
  const { t } = useAppSettings();
  const [open, setOpen] = useState(defaultOpen);

  if (data == null) return null;

  return (
    <div className="idx-collapse">
      <button
        type="button"
        className="idx-collapse__toggle"
        aria-expanded={open}
        onClick={() => setOpen((v) => !v)}
      >
        <span>{open ? t("ops.rawJson.collapse") : t("ops.rawJson.expand")}</span>
        <span aria-hidden>{open ? "▾" : "▸"}</span>
      </button>
      {open ? (
        <div className="idx-collapse__body">
          <pre className="idx-json-pre">{JSON.stringify(redactForDisplay(data), null, 2)}</pre>
        </div>
      ) : null}
    </div>
  );
}
