import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';

import en from '../../i18n/en.json';
import { LanguageProvider } from '../../i18n/LanguageProvider';
import { BackendError, type BackendErrorKind } from '../../services/cdf/backendErrors';

import { BackendErrorPanel } from './BackendErrorPanel';
import { presentBackendError } from './backendErrorPresentation';

const messages: Record<string, string> = en;

function renderPanel(error: unknown, onRetry?: () => void) {
  return render(
    <LanguageProvider>
      <BackendErrorPanel error={error} onRetry={onRetry} />
    </LanguageProvider>
  );
}

const KINDS: BackendErrorKind[] = [
  'host-unavailable',
  'unauthenticated',
  'permission-denied',
  'stream-missing',
  'schema-missing',
  'window-too-long',
  'no-completed-run',
  'unsupported-operation',
  'request-failed',
  'network',
];

describe(BackendErrorPanel.name, () => {
  it.each(KINDS)('explains a %s failure in words, with a title and a hint', (kind) => {
    renderPanel(new BackendError(kind, 'technical text'));

    expect(screen.getByRole('alert')).toBeInTheDocument();
    expect(screen.getByRole('heading')).toHaveTextContent(messages[`backend.error.${kind}.title`] ?? 'MISSING');
    expect(screen.getByTestId('backend-error-details')).toHaveTextContent('technical text');
  });

  it('tells apart an empty stream from runs that did not finish', () => {
    const empty = presentBackendError(new BackendError('no-completed-run', 'x', { detail: 'empty' }));
    const incomplete = presentBackendError(new BackendError('no-completed-run', 'x', { detail: 'incomplete' }));
    const unknown = presentBackendError(new BackendError('no-completed-run', 'x'));

    expect(messages[empty.hintKey]).toContain('no quality run has finished');
    expect(messages[incomplete.hintKey]).toContain('none of them finished');
    expect(messages[incomplete.titleKey]).toBe('A quality run started but did not finish');
    expect(unknown.hintKey).toBe('backend.error.no-completed-run.hint');
  });

  it('says the missing backend must be deployed elsewhere, and never offers to create it', () => {
    renderPanel(new BackendError('stream-missing', 'x', { subject: 'cq_stream' }));

    expect(screen.getByText(/cannot create it/)).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /create|deploy|provision/i })).not.toBeInTheDocument();
  });

  it('names the capability a user without access needs', () => {
    renderPanel(new BackendError('permission-denied', 'x'));

    expect(screen.getByTestId('backend-error-steps')).toHaveTextContent(/streamRecordsAcl READ/);
  });

  it.each(KINDS)('gives a %s failure ordered next steps, every one of them translated', (kind) => {
    const presentation = presentBackendError(new BackendError(kind, 'x', kind === 'no-completed-run' ? { detail: 'empty' } : {}));

    expect(presentation.stepKeys.length).toBeGreaterThanOrEqual(2);
    for (const key of presentation.stepKeys) {
      expect(messages[key], key).toBeDefined();
    }
  });

  it('shows the steps as a numbered list under a Next steps heading', () => {
    renderPanel(new BackendError('network', 'x'));

    const steps = screen.getByTestId('backend-error-steps');
    expect(steps).toHaveTextContent('Next steps');
    expect(steps.querySelectorAll('ol > li').length).toBeGreaterThanOrEqual(2);
  });

  it('treats a stream with no run yet as information, with a way to Configuration, and not as a failure', async () => {
    const onGoToConfiguration = vi.fn();
    render(
      <LanguageProvider>
        <BackendErrorPanel
          error={new BackendError('no-completed-run', 'x', { detail: 'empty' })}
          onRetry={vi.fn()}
          onGoToConfiguration={onGoToConfiguration}
        />
      </LanguageProvider>
    );

    expect(screen.getByRole('alert')).toHaveAttribute('data-tone', 'info');
    expect(screen.getByRole('heading')).toHaveTextContent('No quality run has finished yet');
    expect(screen.getByTestId('backend-error-steps')).toHaveTextContent('Run with this file');
    expect(screen.getByTestId('backend-error-steps')).toHaveTextContent('blue banner');
    await userEvent.setup().click(screen.getByTestId('backend-error-configuration'));
    expect(onGoToConfiguration).toHaveBeenCalledTimes(1);
    expect(screen.getByTestId('backend-error-retry')).toBeInTheDocument();
  });

  it('says who has to start the run when this build cannot', () => {
    render(
      <LanguageProvider>
        <BackendErrorPanel
          error={new BackendError('no-completed-run', 'x', { detail: 'empty' })}
          onGoToConfiguration={vi.fn()}
          canStartRun={false}
        />
      </LanguageProvider>
    );

    expect(screen.getByTestId('backend-error-steps')).toHaveTextContent('This dashboard cannot start a run.');
    expect(screen.getByTestId('backend-error-steps')).not.toHaveTextContent('Run with this file');
  });

  it('points a run that did not finish at the failed execution, and still offers Configuration', () => {
    render(
      <LanguageProvider>
        <BackendErrorPanel
          error={new BackendError('no-completed-run', 'x', { detail: 'incomplete' })}
          onGoToConfiguration={vi.fn()}
        />
      </LanguageProvider>
    );

    expect(screen.getByRole('heading')).toHaveTextContent('A quality run started but did not finish');
    expect(screen.getByTestId('backend-error-steps')).toHaveTextContent('look at the execution that failed');
    expect(screen.getByTestId('backend-error-configuration')).toBeInTheDocument();
  });

  it.each<BackendErrorKind>(['stream-missing', 'schema-missing', 'permission-denied', 'network', 'request-failed'])(
    'does not offer Go to Configuration for a %s failure, because it would fail the same way',
    (kind) => {
      render(
        <LanguageProvider>
          <BackendErrorPanel error={new BackendError(kind, 'x')} onRetry={vi.fn()} onGoToConfiguration={vi.fn()} />
        </LanguageProvider>
      );

      expect(screen.queryByTestId('backend-error-configuration')).not.toBeInTheDocument();
      expect(screen.getByRole('alert')).toHaveAttribute('data-tone', 'error');
    }
  );

  it('retries when trying again can help', async () => {
    const onRetry = vi.fn();
    renderPanel(new BackendError('network', 'x'), onRetry);

    await userEvent.setup().click(screen.getByTestId('backend-error-retry'));

    expect(onRetry).toHaveBeenCalledTimes(1);
  });

  it.each<BackendErrorKind>(['window-too-long', 'schema-missing'])(
    'does not offer to retry a %s failure, because only a configuration change fixes it',
    (kind) => {
      renderPanel(new BackendError(kind, 'x'), vi.fn());

      expect(screen.queryByTestId('backend-error-retry')).not.toBeInTheDocument();
    }
  );

  it('shows a generic explanation, with the details, for an error that is not a backend error', () => {
    renderPanel(new Error('something odd'));

    expect(screen.getByRole('heading')).toHaveTextContent(messages['backend.error.unknown.title'] ?? 'MISSING');
    expect(screen.getByTestId('backend-error-details')).toHaveTextContent('something odd');
  });
});
