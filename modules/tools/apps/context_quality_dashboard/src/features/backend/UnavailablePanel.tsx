import { useLanguage } from '../../i18n/useLanguage';
import { sectionHeadingClassName } from '../shell/sectionHeading';

type UnavailablePanelProps = {
  messageKey: string;
};

/** Stands in for a page or section the connected backend cannot provide, instead of an empty or fabricated one. */
export function UnavailablePanel({ messageKey }: UnavailablePanelProps) {
  const { t } = useLanguage();

  return (
    <section className="rounded-md border border-border p-6 text-center" data-testid="backend-unavailable">
      <h2 className={sectionHeadingClassName}>{t('backend.unavailable.title')}</h2>
      <p className="mt-1 text-sm text-muted-foreground">{t(messageKey)}</p>
    </section>
  );
}
