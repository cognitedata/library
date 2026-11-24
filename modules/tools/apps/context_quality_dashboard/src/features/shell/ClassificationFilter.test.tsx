import { render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';

import { translate } from '../../i18n/messages';

import { ClassificationFilter } from './ClassificationFilter';
import type { Classified } from './classificationFilterModel';

const ITEMS: Classified[] = [{ classes: { assetType: 'Pump' } }, { classes: { assetType: 'Valve' } }];

describe(ClassificationFilter.name, () => {
  it('renders nothing when no item is classified', () => {
    renderFilter([{ classes: {} }], {});

    expect(screen.queryByTestId('classification-filter')).not.toBeInTheDocument();
  });

  it('offers the kind filter first and the value filter only once a kind is chosen', () => {
    const { rerender } = renderFilter(ITEMS, {});

    expect(screen.getByLabelText('Classify by')).toBeInTheDocument();
    expect(screen.queryByLabelText('Class')).not.toBeInTheDocument();

    rerender(element(ITEMS, { kind: 'assetType' }, vi.fn()));

    expect(screen.getByLabelText('Class')).toBeInTheDocument();
  });

  it('shows the chosen value together with its count', () => {
    renderFilter(ITEMS, { kind: 'assetType', value: 'Pump' });

    expect(screen.getByText('Pump (1)')).toBeInTheDocument();
  });

  it('is controlled: it shows the kind it is given', () => {
    renderFilter(ITEMS, { kind: 'assetType' });

    expect(screen.getByText('Asset type')).toBeInTheDocument();
  });
});

function element(items: Classified[], filter: { kind?: string; value?: string }, onChange: () => void) {
  return <ClassificationFilter items={items} filter={filter} onChange={onChange} t={(key: string) => translate(key, 'en')} />;
}

function renderFilter(items: Classified[], filter: { kind?: string; value?: string }) {
  return render(element(items, filter, vi.fn()));
}
