import { render, screen } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { describe, expect, it, vi } from 'vitest';

import en from '../../i18n/en.json';

import { BuiltInChecksCoverageSidePanel } from './BuiltInChecksCoverageSidePanel';

const messages: Record<string, string> = en;

describe(BuiltInChecksCoverageSidePanel.name, () => {
  it('should list built-in dimensions when open', () => {
    render(
      <BuiltInChecksCoverageSidePanel
        open
        onClose={vi.fn()}
        t={(key) => messages[key] ?? key}
      />
    );

    expect(screen.getByTestId('built-in-checks-coverage-panel')).toBeInTheDocument();
    expect(screen.getByText(messages['dimension.asset-hierarchy'] ?? '')).toBeInTheDocument();
    expect(screen.getByTestId('built-in-check-dimension-time-series')).toBeInTheDocument();
  });

  it('should call onClose when close is clicked', async () => {
    const user = userEvent.setup();
    const onClose = vi.fn();
    render(
      <BuiltInChecksCoverageSidePanel open onClose={onClose} t={(key) => messages[key] ?? key} />
    );

    await user.click(screen.getByTestId('built-in-checks-coverage-close'));
    expect(onClose).toHaveBeenCalled();
  });

  it('should render nothing when closed', () => {
    const { container } = render(
      <BuiltInChecksCoverageSidePanel open={false} onClose={vi.fn()} t={(key) => key} />
    );
    expect(container).toBeEmptyDOMElement();
  });
});
