import { render, screen } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { AtlasSparklesIcon } from './AtlasSparklesIcon';

describe(AtlasSparklesIcon.name, () => {
  it('renders the branded Atlas icon by default', () => {
    render(
      <span aria-label="Atlas">
        <AtlasSparklesIcon />
      </span>
    );

    expect(screen.getByLabelText('Atlas').querySelector('img')).toHaveClass('size-3.5');
    expect(screen.getByLabelText('Atlas').querySelector('img')).not.toHaveClass('invert');
  });

  it('inverts the icon for primary buttons', () => {
    render(
      <span aria-label="Atlas">
        <AtlasSparklesIcon filled />
      </span>
    );

    expect(screen.getByLabelText('Atlas').querySelector('img')).toHaveClass('size-3.5', 'invert');
  });
});
