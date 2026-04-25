import { useI18n } from "@/shared/i18n";

export type PermissionsAccessInfoInputProps = {
  idSuffix: string;
  uploading: boolean;
  onFiles: (files: FileList | null) => void;
  pasteText: string;
  onPasteText: (value: string) => void;
  pasteDisplayName: string;
  onPasteDisplayName: (value: string) => void;
  onPasteAdd: () => void;
  uploadError: string | null;
};

export function PermissionsAccessInfoInput({
  idSuffix,
  uploading,
  onFiles,
  pasteText,
  onPasteText,
  pasteDisplayName,
  onPasteDisplayName,
  onPasteAdd,
  uploadError,
}: PermissionsAccessInfoInputProps) {
  const { t } = useI18n();
  const pasteNameId = `permissions-paste-name-${idSuffix}`;

  return (
    <div className="flex flex-col gap-3">
      <div className="flex flex-wrap items-center gap-3">
        <label className="text-sm font-medium text-slate-700">{t("permissions.upload.label")}</label>
        <input
          type="file"
          accept="application/json"
          multiple
          disabled={uploading}
          className="rounded-md border border-slate-300 bg-white px-3 py-2 text-sm text-slate-700 shadow-sm hover:border-slate-400"
          onChange={(event) => onFiles(event.target.files)}
        />
        {uploading ? (
          <span className="text-xs text-slate-500">{t("permissions.upload.uploading")}</span>
        ) : null}
      </div>
      <div
        className="flex flex-col gap-2 rounded-md border border-dashed border-slate-300 bg-slate-50/80 p-3"
        onDragOver={(event) => {
          event.preventDefault();
          event.dataTransfer.dropEffect = "copy";
        }}
        onDrop={(event) => {
          event.preventDefault();
          if (event.dataTransfer.files?.length) {
            void onFiles(event.dataTransfer.files);
          }
        }}
      >
        <div className="text-sm font-medium text-slate-700">{t("permissions.paste.label")}</div>
        <textarea
          value={pasteText}
          onChange={(event) => onPasteText(event.target.value)}
          spellCheck={false}
          rows={8}
          placeholder={t("permissions.paste.placeholder")}
          className="min-h-[120px] w-full resize-y rounded-md border border-slate-300 bg-white px-3 py-2 font-mono text-xs text-slate-800 shadow-sm focus:border-slate-400 focus:outline-none"
        />
        <div className="flex flex-wrap items-end gap-3">
          <div className="flex min-w-[200px] flex-1 flex-col gap-1">
            <label htmlFor={pasteNameId} className="text-xs font-medium text-slate-600">
              {t("permissions.paste.displayName")}
            </label>
            <input
              id={pasteNameId}
              type="text"
              value={pasteDisplayName}
              onChange={(event) => onPasteDisplayName(event.target.value)}
              className="rounded-md border border-slate-300 bg-white px-3 py-2 text-sm text-slate-700 shadow-sm"
            />
          </div>
          <button
            type="button"
            disabled={!pasteText.trim()}
            onClick={onPasteAdd}
            className="cursor-pointer rounded-md bg-slate-900 px-4 py-2 text-sm font-medium text-white hover:bg-slate-800 disabled:cursor-not-allowed disabled:bg-slate-400"
          >
            {t("permissions.paste.add")}
          </button>
        </div>
        <p className="text-[11px] text-slate-500">{t("permissions.paste.dropHint")}</p>
      </div>
      {uploadError ? (
        <div className="rounded-md border border-red-200 bg-red-50 p-3 text-sm text-red-700">{uploadError}</div>
      ) : null}
    </div>
  );
}
