import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';

import { translate } from '../../i18n/messages';

import { ConfiguredMetricsNotConfiguredEmpty } from './ConfiguredMetricsNotConfiguredEmpty';

describe(ConfiguredMetricsNotConfiguredEmpty.name, () => {
  it('explains configuration and opens the Configuration page', async () => {
    const user = userEvent.setup();
    const onOpenConfiguration = vi.fn();

    render(
      <ConfiguredMetricsNotConfiguredEmpty
        canStartRun={true}
        onOpenConfiguration={onOpenConfiguration}
        t={(key: string) => translate(key, 'en')}
      />
    );

    expect(screen.getByText('Measure your own data models')).toBeInTheDocument();
    expect(screen.getByText(/Upload a configuration file on Configuration/)).toBeInTheDocument();
    await user.click(screen.getByRole('button', { name: 'Open Configuration' }));
    expect(onOpenConfiguration).toHaveBeenCalledTimes(1);
  });

  it('notes when this build cannot start a run', () => {
    render(
      <ConfiguredMetricsNotConfiguredEmpty
        canStartRun={false}
        onOpenConfiguration={vi.fn()}
        t={(key: string) => translate(key, 'en')}
      />
    );

    expect(screen.getByText(/This dashboard cannot start a run/)).toBeInTheDocument();
  });
});
