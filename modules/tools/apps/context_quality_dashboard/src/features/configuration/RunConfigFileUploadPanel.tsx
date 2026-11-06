import { Button } from '@cognite/aura/components/button';
import { Progress } from '@cognite/aura/components/progress';
import { IconFileText, IconUpload, IconX } from '@cognite/aura/icons';
import type { DragEvent, KeyboardEvent } from 'react';

import { formatRunConfigFileSize } from './runConfigFileUploadPresentation';

type RunConfigFileUploadPanelProps = {
  fileName: string | null;
  fileByteLength: number;
  isUploading: boolean;
  onBrowse: () => void;
  onRemove: () => void;
  onFile: (file: File) => void;
  t: (key: string) => string;
  /** Shorter dropzone for onboarding layouts. */
  layout?: 'default' | 'compact';
};

function fill(template: string, values: Record<string, string>): string {
  return template.replace(/\{\{(\w+)\}\}/g, (_match: string, name: string) => values[name] ?? '');
}

function pickConfigFile(files: FileList | undefined): File | null {
  const file = files?.[0];
  return file === undefined ? null : file;
}

export function RunConfigFileUploadPanel({
  fileName,
  fileByteLength,
  isUploading,
  onBrowse,
  onRemove,
  onFile,
  t,
  layout = 'default',
}: RunConfigFileUploadPanelProps) {
  const compact = layout === 'compact';
  const onDrop = (event: DragEvent<HTMLDivElement>): void => {
    event.preventDefault();
    const file = pickConfigFile(event.dataTransfer.files);
    if (file !== null) {
      onFile(file);
    }
  };

  const onKeyDown = (event: KeyboardEvent<HTMLDivElement>): void => {
    if (event.key === 'Enter' || event.key === ' ') {
      event.preventDefault();
      onBrowse();
    }
  };

  if (fileName === null) {
    return (
      <div
        role="button"
        tabIndex={0}
        data-testid="run-config-upload-dropzone"
        className={`flex w-full flex-col items-center justify-center rounded-lg border border-dashed border-border bg-muted/20 px-6 text-center outline-none focus-visible:ring-2 focus-visible:ring-ring ${
          compact ? 'min-h-[200px] max-h-[220px] py-6' : 'py-10'
        }`}
        onDragOver={(event) => event.preventDefault()}
        onDrop={onDrop}
        onKeyDown={onKeyDown}
        onClick={onBrowse}
      >
        <IconUpload className={`text-muted-foreground ${compact ? 'size-8' : 'size-10'}`} aria-hidden="true" />
        <p className="mt-3 text-sm font-medium text-foreground">{t('configuration.file.dropzoneTitle')}</p>
        <p className="mt-1 max-w-sm text-xs text-muted-foreground">{t('configuration.file.dropzoneHint')}</p>
        <Button
          type="button"
          variant="outline"
          size="sm"
          className="mt-4"
          onClick={(event) => {
            event.stopPropagation();
            onBrowse();
          }}
        >
          {t('configuration.file.browse')}
        </Button>
      </div>
    );
  }

  const sizeLabel = formatRunConfigFileSize(fileByteLength);
  const statusText = isUploading
    ? t('configuration.file.uploading')
    : fill(t('configuration.file.uploadedReady'), { size: sizeLabel });

  return (
    <div
      className="rounded-lg border border-border bg-background p-4"
      data-testid="run-config-uploaded-file"
    >
      <div className="flex items-start gap-3">
        <span
          className="flex size-10 shrink-0 items-center justify-center rounded-md border border-border bg-muted/40"
          aria-hidden="true"
        >
          <IconFileText className="size-5 text-muted-foreground" />
        </span>
        <div className="min-w-0 flex-1">
          <p className="truncate text-sm font-medium text-foreground">{fileName}</p>
          <p className="mt-0.5 text-xs text-muted-foreground">{statusText}</p>
          {isUploading ? (
            <Progress
              value={null}
              size="sm"
              className="mt-3"
              aria-label={t('configuration.file.uploadProgressLabel')}
            />
          ) : null}
        </div>
        <Button
          type="button"
          variant="ghost"
          size="icon-sm"
          disabled={isUploading}
          aria-label={t('configuration.file.remove')}
          onClick={onRemove}
        >
          <IconX className="size-4" />
        </Button>
      </div>
    </div>
  );
}
