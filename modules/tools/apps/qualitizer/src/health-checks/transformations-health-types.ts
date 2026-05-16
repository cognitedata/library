export type NoopTransformation = {
  id: string;
  name: string;
  writes: number;
  noops: number;
};

export type DmvInconsistency = {
  modelKey: string;
  space: string;
  externalId: string;
  usages: Array<{
    transformationId: string;
    transformationName: string;
    version: string | undefined;
  }>;
};

export const TRANSFORMATIONS_HEALTH_TX_PAGE_SIZE = 100;
