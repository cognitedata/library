import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';

import { translate } from '../../i18n/messages';

import { RunStatusBanner } from './RunStatusBanner';
import { useRunBannerViewModel } from './useRunBannerViewModel';

// Mocked: the banner is presentational, so each test hands it the view model it should render instead of driving run state.
vi.mock('./useRunBannerViewModel', () => ({
  useRunBannerViewModel: vi.fn(),
}));

describe(RunStatusBanner.name, () => {
  it('should render nothing when the banner is hidden', () => {
    vi.mocked(useRunBannerViewModel).mockReturnValue({
      banner: {
        visible: false,
        variant: 'running',
        source: 'config',
        dismissible: false,
        copy: { titleKey: '', bodyKey: '' },
      },
      title: '',
      body: '',
      line: '',
      dismiss: null,
      primaryActionLabel: '',
      onPrimaryAction: null,
    });

    const { container } = render(<RunStatusBanner />);
    expect(container).toBeEmptyDOMElement();
  });

  it('should show a running banner with status semantics', () => {
    const title = translate('runBanner.config.running.title', 'en');
    const body = translate('runBanner.config.running.body', 'en');
    vi.mocked(useRunBannerViewModel).mockReturnValue({
      banner: {
        visible: true,
        variant: 'running',
        source: 'config',
        dismissible: false,
        copy: {
          titleKey: 'runBanner.config.running.title',
          bodyKey: 'runBanner.config.running.body',
        },
      },
      title,
      body,
      line: `${title} — ${body}`,
      dismiss: null,
      primaryActionLabel: '',
      onPrimaryAction: null,
    });

    render(<RunStatusBanner />);
    expect(screen.getByTestId('run-status-banner')).toBeInTheDocument();
    expect(screen.getByTestId('run-status-banner-running')).toHaveAttribute('role', 'status');
    expect(screen.getByTestId('run-status-banner-running')).toHaveTextContent(/Running quality checks/);
    expect(screen.getByTestId('run-status-banner-running').className).toContain('bg-info-muted-background');
    expect(screen.queryByTestId('run-status-banner-dismiss')).not.toBeInTheDocument();
  });

  it('should dismiss a terminal banner', async () => {
    const user = userEvent.setup();
    const dismiss = vi.fn();
    vi.mocked(useRunBannerViewModel).mockReturnValue({
      banner: {
        visible: true,
        variant: 'success',
        source: 'config',
        dismissible: true,
        copy: {
          titleKey: 'runBanner.config.success.title',
          bodyKey: 'runBanner.config.success.body',
        },
      },
      title: 'Run completed successfully',
      body: 'done',
      line: 'Run completed successfully — done',
      dismiss,
      primaryActionLabel: 'View results',
      onPrimaryAction: null,
    });

    render(<RunStatusBanner />);
    await user.click(screen.getByTestId('run-status-banner-dismiss'));
    expect(dismiss).toHaveBeenCalled();
  });
});
