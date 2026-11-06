import { RerunConfigurationSection } from '../runs/RerunConfigurationSection';
import { sectionHeadingClassName } from '../shell/sectionHeading';

import { FirstRunGuide } from './FirstRunGuide';
import { RunConfigEditorCard } from './RunConfigEditorCard';
import { RunConfigExampleDownload } from './RunConfigExampleDownload';
import { useRunConfigFileViewModel } from './useRunConfigFileViewModel';

export function RunConfigFileSection() {
  const viewModel = useRunConfigFileViewModel();
  const { t, fileName, text } = viewModel;
  const hasConfigDraft = fileName !== null || text.trim() !== '';
  const showExampleHelpBanner = !hasConfigDraft;

  return (
    <div className="space-y-8">
      <section aria-label={t('configuration.file.title')} className="space-y-4">
        <header>
          <h2 className={`${sectionHeadingClassName} mb-2`}>{t('configuration.file.title')}</h2>
          <p className="text-xs text-muted-foreground">{t('configuration.file.hint')}</p>
        </header>

        {viewModel.latestRunConfiguration !== undefined &&
        viewModel.latestRunConfiguration !== null &&
        viewModel.latestRunConfiguration.models.length === 0 ? (
          <FirstRunGuide noRunYet={false} canRun={viewModel.canRun} t={t} />
        ) : null}

        {showExampleHelpBanner ? <RunConfigExampleDownload t={t} /> : null}

        <RunConfigEditorCard viewModel={viewModel} />
      </section>
      <RerunConfigurationSection />
    </div>
  );
}
