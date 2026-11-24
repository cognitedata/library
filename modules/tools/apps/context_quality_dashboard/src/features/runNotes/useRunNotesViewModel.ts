import { useQuery } from '@tanstack/react-query';
import { useCallback, useMemo } from 'react';

import { useAppState } from '../../app/state/useAppState';
import { useLanguage } from '../../i18n/useLanguage';
import { useQualityDashboardService } from '../../services/useServices';

import { presentRunNotes, type RunNote } from './runNotePresentation';

export type RunNotesViewModel = {
  notes: RunNote[];
  isOpen: boolean;
  open: () => void;
  close: () => void;
  t: (key: string) => string;
};

/**
 * The latest run's warnings, as notes. The notes are a side channel: while they load, or if they cannot be read, there are
 * simply none, and no page is blocked or shown an error because of them.
 */
export function useRunNotesViewModel(): RunNotesViewModel {
  const { state, dispatch } = useAppState();
  const { t } = useLanguage();
  const service = useQualityDashboardService();

  const warningsQuery = useQuery({
    queryKey: ['runWarnings'],
    queryFn: () => service.getRunWarnings(),
    retry: false,
  });

  const warnings = warningsQuery.data?.warnings;
  const notes = useMemo(() => (warnings === undefined ? [] : presentRunNotes(warnings, t)), [warnings, t]);

  const open = useCallback(() => {
    dispatch({ type: 'setRunNotesOpen', open: true });
  }, [dispatch]);
  const close = useCallback(() => {
    dispatch({ type: 'setRunNotesOpen', open: false });
  }, [dispatch]);

  return { notes, isOpen: state.runNotesOpen === true && notes.length > 0, open, close, t };
}
