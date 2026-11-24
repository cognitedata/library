import { Button } from '@cognite/aura/components/button';

import { useLanguage } from '../../i18n/useLanguage';

import { useDownloadUnlinkedViewModel } from './useDownloadUnlinkedViewModel';

/** Downloads the workbook with every unlinked item of the latest run, one sheet per metric. */
export function DownloadUnlinkedButton() {
  const { t } = useLanguage();
  const download = useDownloadUnlinkedViewModel();

  const unavailable = download.data === false;
  const label = download.isPending
    ? t('download.unlinked.preparing')
    : download.isError
      ? t('download.unlinked.failed')
      : unavailable
        ? t('download.unlinked.unavailable')
        : t('download.unlinked.label');

  return (
    <Button
      variant="outline"
      size="sm"
      onClick={() => download.mutate()}
      disabled={download.isPending}
      title={unavailable ? t('download.unlinked.unavailableHint') : undefined}
      data-testid="download-unlinked"
    >
      {label}
    </Button>
  );
}
