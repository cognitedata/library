import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it } from 'vitest';

import type { AppServices } from '../../services/createServices';
import { createMockServices } from '../../services/mock/createMockServices';
import { renderOverview } from '../../test/renderOverview';
import { withQualityService } from '../../test/withQualityService';
import type { RunWarningDto, RunWarningsDto } from '../../types';

import { RunNotes } from './RunNotes';

function warning(partial: Partial<RunWarningDto> & { code: string }): RunWarningDto {
  return { message: '', subject: null, reason: null, counts: {}, codes: [], ...partial };
}

function servicesWith(getRunWarnings: () => Promise<RunWarningsDto>): AppServices {
  return withQualityService(createMockServices({ latency: 0 }), { getRunWarnings });
}

const result = (warnings: RunWarningDto[]) => () => Promise.resolve({ runTimestamp: '2026-10-12T06:00:00.000Z', warnings });

describe(RunNotes.name, () => {
  beforeEach(() => {
    localStorage.setItem('context-quality-dashboard.language', 'en');
  });

  it('shows a button with the number of notes and opens a neutral panel that lists them', async () => {
    const user = userEvent.setup();
    renderOverview(<RunNotes />, {
      services: servicesWith(
        result([
          warning({ code: 'unmapped-asset-types', counts: { untyped: 3, unmapped: 2 }, codes: ['widget'] }),
          warning({ code: 'mapping-empty', subject: 'tierByTypeCode' }),
        ])
      ),
    });

    const button = await screen.findByTestId('run-notes-button');
    expect(button).toHaveTextContent('Run notes (2)');
    await user.click(button);

    const panel = await screen.findByTestId('run-notes-panel');
    expect(screen.getAllByTestId('run-note')).toHaveLength(2);
    expect(panel).toHaveTextContent('3 assets have no type and 2 have a type that is not mapped to a tier');
    expect(panel).toHaveTextContent('Types: widget');
    expect(panel).toHaveTextContent('tierByTypeCode is not configured');
    for (const note of screen.getAllByTestId('run-note')) {
      expect(note.outerHTML).not.toMatch(/destructive|warning|error/i);
    }
  });

  it('closes with the close button and with Escape', async () => {
    const user = userEvent.setup();
    renderOverview(<RunNotes />, { services: servicesWith(result([warning({ code: 'parents-not-loaded', counts: { assets: 1 } })])) });

    await user.click(await screen.findByTestId('run-notes-button'));
    await user.click(await screen.findByRole('button', { name: 'Close run notes' }));
    expect(screen.queryByTestId('run-notes-panel')).not.toBeInTheDocument();

    await user.click(screen.getByTestId('run-notes-button'));
    await user.keyboard('{Escape}');
    expect(screen.queryByTestId('run-notes-panel')).not.toBeInTheDocument();
  });

  it('restores an open panel from the host state', async () => {
    renderOverview(<RunNotes />, {
      services: servicesWith(result([warning({ code: 'parents-not-loaded', counts: { assets: 1 } })])),
      hostInitialState: JSON.stringify({ page: 'overview', runNotesOpen: true }),
    });

    expect(await screen.findByTestId('run-notes-panel')).toBeInTheDocument();
  });

  it('shows an unknown code and legacy free text as plain notes without failing', async () => {
    const user = userEvent.setup();
    renderOverview(<RunNotes />, {
      services: servicesWith(
        result([warning({ code: 'from-the-future', message: 'Something new' }), warning({ code: 'free-text', message: 'old style note' })])
      ),
    });

    await user.click(await screen.findByTestId('run-notes-button'));

    expect(await screen.findByText('Note: Something new')).toBeInTheDocument();
    expect(screen.getByText('old style note')).toBeInTheDocument();
  });

  it('shows nothing when the run reported no warnings, and does not claim there were none', async () => {
    const { container } = renderOverview(<RunNotes />, { services: servicesWith(result([])) });

    await waitFor(() => {
      expect(container).toBeEmptyDOMElement();
    });
  });

  it('shows nothing, and breaks nothing, when the warnings cannot be read', async () => {
    const { container } = renderOverview(<RunNotes />, {
      services: servicesWith(() => Promise.reject(new Error('stream unavailable'))),
    });

    await new Promise((resolve) => {
      setTimeout(resolve, 50);
    });
    expect(container).toBeEmptyDOMElement();
  });

  it('never prints an entity id', async () => {
    const user = userEvent.setup();
    renderOverview(<RunNotes />, {
      services: servicesWith(result([warning({ code: 'parents-not-loaded', counts: { assets: 2 } })])),
    });

    await user.click(await screen.findByTestId('run-notes-button'));

    expect((await screen.findByTestId('run-notes-panel')).textContent).not.toMatch(/asset-[a-z0-9]+/i);
  });
});
