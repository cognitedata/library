import { useRef, type ReactNode } from "react";
import { createPortal } from "react-dom";
import { useAppSettings } from "../context/AppSettingsContext";
import { useModalDialog } from "../hooks/useModalDialog";

type Props = {
  open: boolean;
  onClose: () => void;
  titleId: string;
  describedBy?: string;
  closeOnEscape?: boolean;
  backdropClassName?: string;
  dialogClassName?: string;
  children: ReactNode;
};

export function ModalDialogShell({
  open,
  onClose,
  titleId,
  describedBy,
  closeOnEscape = true,
  backdropClassName = "gov-modal-backdrop",
  dialogClassName = "gov-modal",
  children,
}: Props) {
  const { t } = useAppSettings();
  const dialogRef = useRef<HTMLDivElement>(null);
  useModalDialog({ open, onClose, dialogRef, closeOnEscape });

  if (!open) return null;

  return createPortal(
    <div className={backdropClassName}>
      <button
        type="button"
        className="gov-modal-backdrop__dismiss"
        aria-label={t("btn.cancel")}
        onClick={onClose}
      />
      <div
        ref={dialogRef}
        className={dialogClassName}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        aria-describedby={describedBy}
      >
        {children}
      </div>
    </div>,
    document.body
  );
}
