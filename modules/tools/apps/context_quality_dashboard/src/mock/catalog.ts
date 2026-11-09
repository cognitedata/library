import type {
  DataOwner,
  Dimension,
  Discipline,
  Division,
  Facility,
} from '../types';

export const FACILITIES: readonly Facility[] = [
  { id: 'demo-north', name: 'Demo North' },
  { id: 'demo-central', name: 'Demo Central' },
  { id: 'demo-south', name: 'Demo South' },
];

export const DIVISIONS: readonly Division[] = [
  { id: 'prod-a', name: 'Production A' },
  { id: 'prod-b', name: 'Production B' },
];

export const DISCIPLINES: readonly Discipline[] = [
  { id: 'mechanical', name: 'Mechanical' },
  { id: 'electrical', name: 'Electrical' },
  { id: 'instrumentation', name: 'Instrumentation' },
];

export const DATA_OWNERS: readonly DataOwner[] = [
  { id: 'sap-steward', name: 'SAP Data Steward', contactLabel: 'sap.steward@example.test' },
  {
    id: 'instrumentation-lead',
    name: 'Instrumentation Lead',
    contactLabel: 'instrumentation.lead@example.test',
  },
  {
    id: 'pid-review-lead',
    name: 'P&ID Review Lead',
    contactLabel: 'pid.review@example.test',
  },
  {
    id: 'model-3d-lead',
    name: '3D Model Lead',
    contactLabel: 'model3d.lead@example.test',
  },
];

export const DIMENSIONS: readonly Dimension[] = [
  {
    id: 'asset-hierarchy',
    name: 'Asset Hierarchy',
    description: 'Hierarchy completion, orphans, and type coverage.',
    optional: false,
  },
  {
    id: 'equipment-asset',
    name: 'Equipment & Asset',
    description: 'Equipment linkage, coverage, and completeness.',
    optional: false,
  },
  {
    id: 'time-series',
    name: 'Time Series',
    description: 'TS-to-asset linkage, freshness, and monitoring coverage.',
    optional: false,
  },
  {
    id: 'maintenance',
    name: 'Maintenance Workflow',
    description: 'Work order and notification contextualization.',
    optional: true,
  },
  {
    id: 'file-annotation',
    name: 'File Annotation',
    description: 'Diagram annotation confidence and review queues.',
    optional: true,
  },
  {
    id: 'model-3d',
    name: '3D Model',
    description: '3D object linkage and bounding box completeness.',
    optional: true,
  },
  {
    id: 'files',
    name: 'Files',
    description: 'File-to-asset linkage and metadata completeness.',
    optional: false,
  },
];

export const OWNER_PREFIX_RULES: readonly { prefix: string; ownerId: string }[] = [
  { prefix: 'PI-COMP3-', ownerId: 'instrumentation-lead' },
  { prefix: 'P&ID-', ownerId: 'pid-review-lead' },
  { prefix: '3D-', ownerId: 'model-3d-lead' },
];
