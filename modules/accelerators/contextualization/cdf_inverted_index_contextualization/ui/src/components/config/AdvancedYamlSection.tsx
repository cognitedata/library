import CodeMirror from "@uiw/react-codemirror";
import { yaml } from "@codemirror/lang-yaml";
import { oneDark } from "@codemirror/theme-one-dark";
import { useEffect, useState } from "react";
import { useAppSettings } from "../../context/AppSettingsContext";
import { SectionIntro } from "../shared/SectionIntro";

type Props = {
  yamlText: string;
  onApply: (content: string) => void;
};

export function AdvancedYamlSection({ yamlText, onApply }: Props) {
  const { t, resolvedTheme } = useAppSettings();
  const [open, setOpen] = useState(false);
  const [draft, setDraft] = useState(yamlText);

  useEffect(() => {
    if (!open) setDraft(yamlText);
  }, [yamlText, open]);

  const apply = () => {
    onApply(draft);
    setOpen(false);
  };

  const cancel = () => {
    setDraft(yamlText);
    setOpen(false);
  };

  return (
    <section className="idx-config-advanced idx-config-advanced--expert">
      <button
        type="button"
        className="idx-config-advanced__toggle"
        aria-expanded={open}
        onClick={() => setOpen((o) => !o)}
      >
        {open ? t("config.advanced.collapse") : t("config.advanced.expand")}
      </button>
      {open ? (
        <div className="idx-config-advanced__body">
          <SectionIntro variant="expert">{t("config.advanced.hint")}</SectionIntro>
          <div className="idx-config-editor idx-config-editor--advanced">
            <CodeMirror
              value={draft}
              height="min(50vh, 24rem)"
              theme={resolvedTheme === "dark" ? oneDark : "light"}
              extensions={[yaml()]}
              onChange={(v) => setDraft(v)}
            />
          </div>
          <div className="idx-field-row">
            <button type="button" className="idx-btn idx-btn--primary" onClick={apply}>
              {t("config.advanced.apply")}
            </button>
            <button type="button" className="idx-btn" onClick={cancel}>
              {t("config.advanced.cancel")}
            </button>
          </div>
        </div>
      ) : null}
    </section>
  );
}
