import { Button } from '@cognite/aura/components/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@cognite/aura/components/dialog';
import { IconBuilding, IconClock, IconInfoCircle } from '@cognite/aura/icons';
import type { ComponentType } from 'react';

import { interpolate } from '../../i18n/interpolate';
import { useLanguage } from '../../i18n/useLanguage';

import { requestKey, type RunRequest } from './runRequest';

type ConfirmRunDialogProps = {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  request: RunRequest;
  /** Set when a run is going on: the dialog then says so and does not offer to start another. */
  blocked: 'own' | 'other' | null;
  onConfirm: () => void;
};

/**
 * Asks before a run is started: a run reads a lot of data and puts load on the project, so a click by mistake must not
 * start one. It says what the run measures and what it leaves as it was.
 */
export function ConfirmRunDialog({ open, onOpenChange, request, blocked, onConfirm }: ConfirmRunDialogProps) {
  const { t } = useLanguage();
  const name =
    request.kind === 'dimension'
      ? t(`dimension.${request.dimensionId}`)
      : request.kind === 'configuredView'
        ? request.viewLabel
        : '';
  const count =
    request.kind === 'selection' ? String(request.dimensionIds.length + request.links.length) : '';
  const key = requestKey(request);

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-lg" showCloseButton closeButtonLabel={t('run.confirm.cancel')} data-testid="confirm-run-dialog">
        {blocked === null ? (
          <>
            <DialogHeader>
              <DialogTitle>{interpolate(t(`run.confirm.title.${request.kind}`), { name, count })}</DialogTitle>
              <DialogDescription>
                {interpolate(t(`run.confirm.body.${request.kind}`), { name, count })}
              </DialogDescription>
            </DialogHeader>
            <ConfirmRunBeforeYouStart t={t} />
            <DialogFooter>
              <Button variant="outline" onClick={() => onOpenChange(false)} data-testid={`confirm-run-cancel-${key}`}>
                {t('run.confirm.cancel')}
              </Button>
              <Button
                onClick={() => {
                  onOpenChange(false);
                  onConfirm();
                }}
                data-testid={`confirm-run-start-${key}`}
              >
                {t('run.confirm.start')}
              </Button>
            </DialogFooter>
          </>
        ) : (
          <>
            <DialogHeader>
              <DialogTitle>{t('run.busy.title')}</DialogTitle>
              <DialogDescription data-testid="confirm-run-busy">{t(`run.busy.${blocked}`)}</DialogDescription>
            </DialogHeader>
            <DialogFooter>
              <Button onClick={() => onOpenChange(false)}>{t('run.busy.close')}</Button>
            </DialogFooter>
          </>
        )}
      </DialogContent>
    </Dialog>
  );
}

function ConfirmRunBeforeYouStart({ t }: { t: (key: string) => string }) {
  return (
    <section className="space-y-3" aria-labelledby="confirm-run-before-you-start">
      <h3 id="confirm-run-before-you-start" className="text-sm font-medium text-foreground">
        {t('run.confirm.beforeYouStart')}
      </h3>
      <ul className="space-y-3">
        <ConfirmRunNoteRow icon={IconClock} text={t('run.confirm.note.duration')} />
        <ConfirmRunNoteRow icon={IconBuilding} text={t('run.confirm.note.load')} />
        <ConfirmRunNoteRow icon={IconInfoCircle} text={t('run.confirm.note.noCancel')} />
      </ul>
    </section>
  );
}

function ConfirmRunNoteRow({
  icon: Icon,
  text,
}: {
  icon: ComponentType<{ className?: string }>;
  text: string;
}) {
  return (
    <li className="flex gap-3">
      <Icon className="mt-0.5 size-4 shrink-0 text-muted-foreground" aria-hidden />
      <span className="text-sm text-secondary-foreground">{text}</span>
    </li>
  );
}
