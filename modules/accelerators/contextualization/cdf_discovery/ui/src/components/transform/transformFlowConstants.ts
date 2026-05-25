/** Persistence nodes whose primary ``out`` data edge may target only ``etlEnd``. */
export const etlPersistenceOutboundToEndOnlyRfTypes = new Set<string>([
  "etlSaveView",
  "etlSaveRaw",
  "etlSaveClassic",
]);
