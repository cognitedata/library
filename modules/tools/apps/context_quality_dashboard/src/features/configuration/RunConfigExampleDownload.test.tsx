import { render, screen, within } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { translate } from '../../i18n/messages';

import { RunConfigExampleDownload } from './RunConfigExampleDownload';

describe(RunConfigExampleDownload.name, () => {
  it('renders the help card title and download control without a duplicate title icon', () => {
    render(<RunConfigExampleDownload t={(key: string) => translate(key, 'en')} />);

    const card = screen.getByTestId('run-config-example-card');
    expect(within(card).getByRole('heading', { level: 3, name: 'Need help getting started?' })).toBeInTheDocument();
    expect(within(card).getByRole('button', { name: 'Download example YAML' })).toBeInTheDocument();
    expect(within(card).queryByLabelText('', { selector: '.rounded-full' })).not.toBeInTheDocument();
  });
});
