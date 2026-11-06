import { sectionHeadingClassName } from '../shell/sectionHeading';

import { RerunSelectionCard } from './RerunSelectionCard';
import { RerunSelectionProvider } from './RerunSelectionProvider';
import { useRerunViewModel } from './useRerunViewModel';

function RerunConfigurationBody() {
  const viewModel = useRerunViewModel();
  return (
    <section aria-label={viewModel.t('rerun.title')} className="space-y-4" data-testid="rerun-configuration">
      <header>
        <h2 className={`${sectionHeadingClassName} mb-2`}>{viewModel.t('rerun.title')}</h2>
        <p className="text-xs text-muted-foreground">{viewModel.t('rerun.hint')}</p>
      </header>
      <RerunSelectionCard viewModel={viewModel} />
    </section>
  );
}

export function RerunConfigurationSection() {
  return (
    <RerunSelectionProvider>
      <RerunConfigurationBody />
    </RerunSelectionProvider>
  );
}
