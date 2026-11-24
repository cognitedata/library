import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';

import { translate } from '../../i18n/messages';

import { RunConfigFileReviewDialog } from './RunConfigFileReviewDialog';

const VALID = {
  ok: true as const,
  models: [
    { model: 'cdf_cdm:CogniteCore:v1', links: ['CogniteAsset.type'] },
  ],
};

describe(RunConfigFileReviewDialog.name, () => {
  it('lists models and links when the configuration is valid', () => {
    render(
      <RunConfigFileReviewDialog
        open
        onOpenChange={vi.fn()}
        result={VALID}
        t={(key) => translate(key, 'en')}
      />
    );

    const model = screen.getByTestId('run-config-model-cdf_cdm:CogniteCore:v1');
    expect(model).toHaveTextContent('type');
  });

  it('closes when the user presses Escape', async () => {
    const user = userEvent.setup();
    const onOpenChange = vi.fn();
    render(
      <RunConfigFileReviewDialog
        open
        onOpenChange={onOpenChange}
        result={VALID}
        t={(key) => translate(key, 'en')}
      />
    );

    await user.keyboard('{Escape}');

    expect(onOpenChange).toHaveBeenCalled();
    expect(onOpenChange.mock.calls[0]?.[0]).toBe(false);
  });
});
