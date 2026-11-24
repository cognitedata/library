import { useLanguage } from '../../i18n/useLanguage';
import type { ConfigIssue } from '../../services/cdf/streamConfig';

type BackendConfigErrorScreenProps = {
  issues: readonly ConfigIssue[];
};

/** Shown instead of the app when the build has no valid backend configuration. Nothing is read or created. */
export function BackendConfigErrorScreen({ issues }: BackendConfigErrorScreenProps) {
  const { t } = useLanguage();

  return (
    <main className="mx-auto max-w-2xl space-y-4 p-8" data-testid="backend-config-error">
      <h1 className="text-lg font-semibold">{t('backend.configError.title')}</h1>
      <p className="text-sm text-muted-foreground">{t('backend.configError.intro')}</p>
      <ul className="space-y-2" data-testid="backend-config-issues">
        {issues.map((issue) => (
          <li key={issue.variable} className="rounded-md border border-border p-3 text-sm">
            <code className="font-mono font-medium">{issue.variable}</code>
            <span className="text-muted-foreground">: {issue.message}</span>
          </li>
        ))}
      </ul>
      <p className="text-sm text-muted-foreground">{t('backend.configError.hint')}</p>
    </main>
  );
}
