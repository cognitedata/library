import { useContext } from 'react';

import { QualityOperationsContext } from './qualityOperationsReactContext';

export function useQualityOperations() {
  const context = useContext(QualityOperationsContext);
  if (context === null) {
    throw new Error('useQualityOperations must be used within QualityOperationsProvider');
  }
  return context;
}
