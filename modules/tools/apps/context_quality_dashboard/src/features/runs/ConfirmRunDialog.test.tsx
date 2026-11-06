import { render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { LanguageProvider } from '../../i18n/LanguageProvider';
import { translate } from '../../i18n/messages';

import { ConfirmRunDialog } from './ConfirmRunDialog';

describe(ConfirmRunDialog.name, () => {
  it('should show title, intro, before-you-start notes, and actions for a selection run', () => {
    render(
      <LanguageProvider>
        <ConfirmRunDialog
          open
          onOpenChange={vi.fn()}
          request={{
            kind: 'selection',
            dimensionIds: ['files'],
            links: [{ model: 'm', link: 'V.p' }],
          }}
          blocked={null}
          onConfirm={vi.fn()}
        />
      </LanguageProvider>
    );

    expect(screen.getByTestId('confirm-run-dialog')).toBeInTheDocument();
    expect(screen.getByText('Run 2 selected checks?')).toBeInTheDocument();
    expect(
      screen.getByText(/This will start a new quality measurement for the 2 selected checks/)
    ).toBeInTheDocument();
    expect(screen.getByText(translate('run.confirm.beforeYouStart', 'en'))).toBeInTheDocument();
    expect(screen.getByText(translate('run.confirm.note.duration', 'en'))).toBeInTheDocument();
    expect(screen.getByText(translate('run.confirm.note.load', 'en'))).toBeInTheDocument();
    expect(screen.getByText(translate('run.confirm.note.noCancel', 'en'))).toBeInTheDocument();
    expect(screen.getByTestId('confirm-run-cancel-selection')).toBeInTheDocument();
    expect(screen.getByTestId('confirm-run-start-selection')).toBeInTheDocument();
  });

  it('should show view-specific copy for a configured view rerun', () => {
    render(
      <LanguageProvider>
        <ConfirmRunDialog
          open
          onOpenChange={vi.fn()}
          request={{
            kind: 'configuredView',
            groupId: 'cdf_cdm:CogniteCore:v1/CogniteEquipment',
            viewLabel: 'CogniteEquipment',
            links: [
              { model: 'cdf_cdm:CogniteCore:v1', link: 'CogniteEquipment.equipmentClass' },
              { model: 'cdf_cdm:CogniteCore:v1', link: 'CogniteEquipment.files' },
            ],
          }}
          blocked={null}
          onConfirm={vi.fn()}
        />
      </LanguageProvider>
    );

    expect(screen.getByText('Rerun CogniteEquipment?')).toBeInTheDocument();
    expect(
      screen.getByText(/This will run the CogniteEquipment quality checks again/)
    ).toBeInTheDocument();
    expect(screen.queryByText(/2 selected checks/)).not.toBeInTheDocument();
    expect(
      screen.getByTestId('confirm-run-start-configured-view-cdf_cdm:CogniteCore:v1/CogniteEquipment')
    ).toBeInTheDocument();
  });

  it('should not offer start when a run is already in progress', () => {
    render(
      <LanguageProvider>
        <ConfirmRunDialog
          open
          onOpenChange={vi.fn()}
          request={{ kind: 'everything' }}
          blocked="other"
          onConfirm={vi.fn()}
        />
      </LanguageProvider>
    );

    expect(screen.getByTestId('confirm-run-busy')).toHaveTextContent('scheduled one');
    expect(screen.queryByTestId('confirm-run-start-everything')).not.toBeInTheDocument();
  });
});
