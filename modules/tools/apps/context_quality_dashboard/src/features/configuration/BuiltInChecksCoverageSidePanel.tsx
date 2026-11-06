import { Button } from '@cognite/aura/components/button';
import { useEffect } from 'react';

import { BUILT_IN_CHECK_DIMENSION_IDS } from './builtInCheckDimensions';

type BuiltInChecksCoverageSidePanelProps = {
  open: boolean;
  onClose: () => void;
  t: (key: string) => string;
};

export function BuiltInChecksCoverageSidePanel({ open, onClose, t }: BuiltInChecksCoverageSidePanelProps) {
  useEffect(() => {
    if (!open) {
      return undefined;
    }
    const onKeyDown = (event: KeyboardEvent): void => {
      if (event.key === 'Escape') {
        onClose();
      }
    };
    window.addEventListener('keydown', onKeyDown);
    return () => {
      window.removeEventListener('keydown', onKeyDown);
    };
  }, [open, onClose]);

  if (!open) {
    return null;
  }

  return (
    <aside
      role="dialog"
      aria-modal="true"
      aria-labelledby="built-in-checks-coverage-title"
      data-testid="built-in-checks-coverage-panel"
      className="fixed inset-y-0 right-0 z-30 flex w-full max-w-md flex-col overflow-y-auto border-l border-border bg-card-background p-6 shadow-lg"
    >
      <div className="flex items-start justify-between gap-3 border-b border-border pb-4">
        <div className="min-w-0">
          <h2 id="built-in-checks-coverage-title" className="text-base font-semibold text-foreground">
            {t('configuration.firstRunOnboarding.builtIn.coveragePanel.title')}
          </h2>
          <p className="mt-1 text-sm text-muted-foreground">
            {t('configuration.firstRunOnboarding.builtIn.coveragePanel.intro')}
          </p>
        </div>
        <Button
          variant="ghost"
          size="icon-sm"
          onClick={onClose}
          aria-label={t('configuration.firstRunOnboarding.builtIn.coveragePanel.close')}
          data-testid="built-in-checks-coverage-close"
        >
          ×
        </Button>
      </div>

      <ul className="mt-4 space-y-3">
        {BUILT_IN_CHECK_DIMENSION_IDS.map((dimensionId) => (
          <li
            key={dimensionId}
            className="rounded-lg border border-border bg-background p-3"
            data-testid={`built-in-check-dimension-${dimensionId}`}
          >
            <p className="text-sm font-medium text-foreground">{t(`dimension.${dimensionId}`)}</p>
            <p className="mt-1 text-sm text-muted-foreground">{t(`dimension.description.${dimensionId}`)}</p>
          </li>
        ))}
      </ul>
    </aside>
  );
}
