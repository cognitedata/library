import { useRef, type ReactNode } from "react";
import type { MessageKey } from "../i18n/types";
import { useModalDialog } from "../hooks/useModalDialog";

type TFn = (key: MessageKey, vars?: Record<string, string | number>) => string;

type Props = {
  t: TFn;
  title: string;
  onClose: () => void;
  children: ReactNode;
};

export function DocumentTabFullscreenOverlay({ t, title, onClose, children }: Props) {
  const dialogRef = useRef<HTMLDivElement>(null);
  useModalDialog({ open: true, onClose, dialogRef });

  return (
    <div
      ref={dialogRef}
      className="disc-doc-fullscreen"
      role="dialog"
      aria-modal="true"
      aria-labelledby="disc-doc-fullscreen-title"
    >
      <div className="disc-doc-fullscreen__bar">
        <div className="disc-doc-fullscreen__title-row">
          <h2 id="disc-doc-fullscreen-title" className="disc-doc-fullscreen__title">
            {title}
          </h2>
        </div>
        <button type="button" className="disc-btn" onClick={onClose}>
          {t("tabs.exitFullscreen")}
        </button>
      </div>
      <div className="disc-doc-fullscreen__body">{children}</div>
    </div>
  );
}
