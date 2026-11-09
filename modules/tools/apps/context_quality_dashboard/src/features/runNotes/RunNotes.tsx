import { Button } from '@cognite/aura/components/button';
import { useEffect } from 'react';

import { interpolate } from '../../i18n/interpolate';

import { useRunNotesViewModel } from './useRunNotesViewModel';

/**
 * A compact "Run notes (n)" button and the panel it opens. Hidden when the run reported nothing: an older run carries no
 * warnings, which is not the same as having none, so the dashboard says nothing at all. Informational styling on purpose:
 * the notes describe how the run measured the data, they are not failed metrics.
 */
export function RunNotes() {
  const viewModel = useRunNotesViewModel();
  const { notes, isOpen, close, open, t } = viewModel;

  useEffect(() => {
    if (!isOpen) {
      return undefined;
    }
    const onKeyDown = (event: KeyboardEvent): void => {
      if (event.key === 'Escape') {
        close();
      }
    };
    window.addEventListener('keydown', onKeyDown);
    return () => {
      window.removeEventListener('keydown', onKeyDown);
    };
  }, [isOpen, close]);

  if (notes.length === 0) {
    return null;
  }

  return (
    <>
      <Button variant="outline" size="sm" onClick={open} data-testid="run-notes-button">
        {interpolate(t('runNotes.button'), { count: String(notes.length) })}
      </Button>
      {isOpen ? (
        <aside
          role="dialog"
          aria-modal="true"
          aria-labelledby="run-notes-title"
          data-testid="run-notes-panel"
          className="fixed inset-y-0 right-0 z-30 flex w-full max-w-md flex-col overflow-y-auto border-l border-border bg-card-background p-6 shadow-lg"
        >
          <div className="flex items-center justify-between gap-3 border-b border-border pb-4">
            <h2 id="run-notes-title" className="text-base font-semibold text-foreground">
              {t('runNotes.title')}
            </h2>
            <Button variant="ghost" size="icon-sm" onClick={close} aria-label={t('runNotes.close')}>
              ×
            </Button>
          </div>
          <p className="mt-4 text-sm text-muted-foreground">{t('runNotes.intro')}</p>
          <ul className="mt-4 space-y-3">
            {notes.map((note) => (
              <li
                key={note.id}
                data-testid="run-note"
                className="rounded-lg border border-border bg-background p-3 text-sm text-foreground"
              >
                <p>{note.text}</p>
                {note.detail === null ? null : (
                  <p className="mt-1 text-xs text-muted-foreground">{note.detail}</p>
                )}
              </li>
            ))}
          </ul>
        </aside>
      ) : null}
    </>
  );
}
