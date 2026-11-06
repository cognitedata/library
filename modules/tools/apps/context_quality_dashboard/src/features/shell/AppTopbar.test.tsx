import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';

import { translate } from '../../i18n/messages';

import { AppTopbar } from './AppTopbar';

describe(AppTopbar.name, () => {
  it('renders the product name as a heading regardless of the active page', () => {
    const { unmount } = renderTopbar({ page: 'overview' });
    const titleOnOverview = screen.getByRole('heading', {
      level: 1,
      name: 'Contextualization Quality',
    });
    expect(titleOnOverview).toHaveClass('font-medium', 'text-foreground');
    expect(titleOnOverview).not.toHaveAttribute('data-slot', 'breadcrumb-link');
    unmount();

    renderTopbar({ page: 'dimensions' });
    const titleOnDimensions = screen.getByRole('heading', {
      level: 1,
      name: 'Contextualization Quality',
    });
    expect(titleOnDimensions).toHaveClass('font-medium', 'text-foreground');
  });

  it('renders one tab per page except alerts, and selects the active page', () => {
    renderTopbar({ page: 'dimensions' });

    expect(screen.getAllByRole('tab')).toHaveLength(4);
    expect(screen.getByRole('tab', { name: 'Dimensions', selected: true })).toBeInTheDocument();
    expect(screen.getByRole('tab', { name: 'Issues' })).toBeInTheDocument();
    expect(screen.queryByRole('tab', { name: 'Alerts' })).not.toBeInTheDocument();
  });

  it('does not render page tabs when showPageTabs is false', () => {
    renderTopbar({ showPageTabs: false });

    expect(screen.queryByRole('tab')).not.toBeInTheDocument();
    expect(screen.getByRole('button', { name: 'Settings' })).toBeInTheDocument();
  });

  it('reports the page selected from the tabs', async () => {
    const user = userEvent.setup();
    const onPageChange = vi.fn();
    renderTopbar({ onPageChange });

    await user.click(screen.getByRole('tab', { name: 'Configuration' }));

    expect(onPageChange).toHaveBeenCalledWith('configuration');
  });

  it('does not render Ask Atlas in the top bar', () => {
    renderTopbar({});

    expect(screen.queryByRole('button', { name: 'Ask Atlas' })).not.toBeInTheDocument();
  });

  it('exposes a single settings control for language and theme', () => {
    renderTopbar({});

    expect(screen.getByRole('button', { name: 'Settings' })).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Language' })).not.toBeInTheDocument();
    expect(screen.queryByRole('button', { name: 'Theme' })).not.toBeInTheDocument();
  });

  it('reports the language chosen from the settings menu', async () => {
    const user = userEvent.setup();
    const onLanguageChange = vi.fn();
    renderTopbar({ onLanguageChange });

    await user.click(screen.getByRole('button', { name: 'Settings' }));
    await user.click(await screen.findByRole('menuitemradio', { name: 'Japanese' }));

    expect(onLanguageChange).toHaveBeenCalledWith('ja');
  });

  it('reports the theme chosen from the settings menu', async () => {
    const user = userEvent.setup();
    const onThemeChange = vi.fn();
    renderTopbar({ onThemeChange });

    await user.click(screen.getByRole('button', { name: 'Settings' }));
    await user.click(await screen.findByRole('menuitemradio', { name: 'Dark mode' }));

    expect(onThemeChange).toHaveBeenCalledWith('dark');
  });

  it('keeps the settings menu text in English even when the active language is Japanese', async () => {
    const user = userEvent.setup();
    renderTopbar({ language: 'ja', t: (key: string) => translate(key, 'ja') });

    await user.click(screen.getByRole('button', { name: 'Settings' }));

    expect(await screen.findByText('Language')).toBeInTheDocument();
    expect(screen.getByText('English')).toBeInTheDocument();
    expect(screen.getByText('Japanese')).toBeInTheDocument();
    expect(screen.getByText('Theme')).toBeInTheDocument();
    expect(screen.getByText('Light mode')).toBeInTheDocument();
    expect(screen.getByText('Dark mode')).toBeInTheDocument();
  });

});

type RenderOverrides = Partial<Parameters<typeof AppTopbar>[0]>;

function renderTopbar(overrides: RenderOverrides) {
  return render(
    <AppTopbar
      page="overview"
      language="en"
      themeMode="light"
      onPageChange={vi.fn()}
      onLanguageChange={vi.fn()}
      onThemeChange={vi.fn()}
      t={(key: string) => translate(key, 'en')}
      {...overrides}
    />
  );
}
