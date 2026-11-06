import { AppShell } from '../features/shell/AppShell';
import { LanguageProvider } from '../i18n/LanguageProvider';

import { AppStateProvider } from './state/AppStateProvider';

type ProductShellProps = {
  hostInitialState?: string;
};

export function ProductShell({ hostInitialState }: ProductShellProps) {
  return (
    <LanguageProvider>
      <AppStateProvider hostInitialState={hostInitialState}>
        <AppShell />
      </AppStateProvider>
    </LanguageProvider>
  );
}
