import { useLanguage } from '../../i18n/useLanguage';
import { useBackend } from '../../services/useServices';
import { isFirstRunOnboardingPreview } from '../shell/firstRunOnboardingMode';

/** Visible on every page whenever the built-in sample data is in use, so it is never mistaken for real data. */
export function DemoModeBanner() {
  const { t } = useLanguage();
  const backend = useBackend();

  if (backend.kind !== 'mock' || isFirstRunOnboardingPreview()) {
    return null;
  }
  return (
    <div
      role="status"
      className="bg-warning-muted-background px-4 py-1 text-center text-xs font-medium text-warning-foreground"
      data-testid="demo-mode-banner"
    >
      {t('backend.demo.banner')}
    </div>
  );
}
