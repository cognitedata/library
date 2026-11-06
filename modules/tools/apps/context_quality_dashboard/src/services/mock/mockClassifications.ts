import type { ClassificationGroupDto, ClassificationRowDto } from '../../types';

type ClassificationWeight = {
  id: string;
  name: string;
  nameKey: string;
  descriptionKey: string;
  /** Relative share of the in-scope total this classification represents. */
  shareWeight: number;
  coveragePercent: number;
};

// Demo data for the mock backend only: the CDF backend discovers classifications from the project.
// Each classification's share of the real in-scope total and its coverage
// percent are fixed here; the resulting counts are derived so they always
// reconcile with the dashboard's actual in-scope total (see
// buildClassificationRows), rather than being independent demo numbers.
const CLASSIFICATION_WEIGHTS: readonly ClassificationWeight[] = [
  {
    id: 'rotating-equipment',
    name: 'Rotating equipment',
    nameKey: 'overview.classification.rotatingEquipment',
    descriptionKey: 'overview.classification.rotatingEquipmentExamples',
    shareWeight: 1326,
    coveragePercent: 94,
  },
  {
    id: 'static-equipment',
    name: 'Static equipment',
    nameKey: 'overview.classification.staticEquipment',
    descriptionKey: 'overview.classification.staticEquipmentExamples',
    shareWeight: 902,
    coveragePercent: 95,
  },
  {
    id: 'instrumentation',
    name: 'Instrumentation',
    nameKey: 'overview.classification.instrumentation',
    descriptionKey: 'overview.classification.instrumentationExamples',
    shareWeight: 2274,
    coveragePercent: 94,
  },
  {
    id: 'electrical-equipment',
    name: 'Electrical equipment',
    nameKey: 'overview.classification.electricalEquipment',
    descriptionKey: 'overview.classification.electricalEquipmentExamples',
    shareWeight: 612,
    coveragePercent: 69,
  },
  {
    id: 'documents',
    name: 'Documents',
    nameKey: 'overview.classification.documents',
    descriptionKey: 'overview.classification.documentsExamples',
    shareWeight: 640,
    coveragePercent: 50,
  },
];

export function buildMockClassificationRows(inScope: number): ClassificationRowDto[] {
  const totalWeight = CLASSIFICATION_WEIGHTS.reduce((sum, weight) => sum + weight.shareWeight, 0);

  let allocated = 0;
  return CLASSIFICATION_WEIGHTS.map((weight, index) => {
    const isLast = index === CLASSIFICATION_WEIGHTS.length - 1;
    const totalCount = isLast
      ? inScope - allocated
      : Math.round((inScope * weight.shareWeight) / totalWeight);
    allocated += totalCount;

    return {
      id: weight.id,
      label: weight.name,
      labelKey: weight.nameKey,
      descriptionKey: weight.descriptionKey,
      totalCount,
      contextualisedCount: Math.round((totalCount * weight.coveragePercent) / 100),
      coveragePercent: weight.coveragePercent,
    };
  });
}

export function buildMockClassificationGroups(inScope: number): ClassificationGroupDto[] {
  return [{ kind: 'assetClass', rows: buildMockClassificationRows(inScope) }];
}
