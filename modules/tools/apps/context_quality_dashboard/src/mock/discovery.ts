import type { DiscoveredDataModel, DiscoveredRelationship, DiscoveredView, ViewRef } from '../types';

export const MOCK_DATA_MODELS: readonly DiscoveredDataModel[] = [
  { space: 'rmdm', displayName: 'RMDM' },
  { space: 'cdf_idm', displayName: 'IDM' },
  { space: 'cdf_cdm', displayName: 'CDM' },
];

export const MOCK_VIEWS_BY_SPACE: Readonly<Record<string, readonly DiscoveredView[]>> = {
  rmdm: [
    { space: 'rmdm', externalId: 'IDI_BOM', displayName: 'IDI BOM', instanceCount: 1000 },
    { space: 'rmdm', externalId: 'Equipment', displayName: 'Equipment', instanceCount: 420 },
  ],
  cdf_idm: [
    {
      space: 'cdf_idm',
      externalId: 'CogniteMaintenanceOrder',
      displayName: 'Maintenance Order',
      instanceCount: 3200,
    },
    {
      space: 'cdf_idm',
      externalId: 'CogniteNotification',
      displayName: 'Notification',
      instanceCount: 2800,
    },
  ],
  cdf_cdm: [
    { space: 'cdf_cdm', externalId: 'CogniteAsset', displayName: 'Asset', instanceCount: 5400 },
    { space: 'cdf_cdm', externalId: 'CogniteEquipment', displayName: 'Equipment', instanceCount: 2100 },
    { space: 'cdf_cdm', externalId: 'CogniteDiagramAnnotation', displayName: 'Diagram Annotation', instanceCount: 890 },
  ],
};

function viewKey(view: ViewRef): string {
  return `${view.space}:${view.externalId}`;
}

function buildRelationship(
  sourceView: ViewRef,
  targetView: ViewRef,
  targetDisplayName: string,
  description: string,
  discoveredCount: number
): DiscoveredRelationship {
  const relationshipId = `${viewKey(sourceView)}->${viewKey(targetView)}`;
  return {
    relationshipId,
    sourceView,
    targetView,
    targetDisplayName,
    description,
    discoveredCount,
  };
}

const IDI_BOM: ViewRef = { space: 'rmdm', externalId: 'IDI_BOM' };
const RMDM_EQUIPMENT: ViewRef = { space: 'rmdm', externalId: 'Equipment' };
const CDM_ASSET: ViewRef = { space: 'cdf_cdm', externalId: 'CogniteAsset' };
const CDM_EQUIPMENT: ViewRef = { space: 'cdf_cdm', externalId: 'CogniteEquipment' };
const IDM_MAINTENANCE_ORDER: ViewRef = { space: 'cdf_idm', externalId: 'CogniteMaintenanceOrder' };
const CDM_LOCATION: ViewRef = { space: 'cdf_cdm', externalId: 'CogniteLocation' };

export const MOCK_RELATIONSHIPS_BY_SOURCE: Readonly<Record<string, readonly DiscoveredRelationship[]>> = {
  [viewKey(IDI_BOM)]: [
    buildRelationship(IDI_BOM, CDM_ASSET, 'Asset', 'IDI BOM linked to Cognite Asset', 812),
    buildRelationship(IDI_BOM, CDM_EQUIPMENT, 'Equipment', 'IDI BOM linked to Cognite Equipment', 695),
    buildRelationship(
      IDI_BOM,
      IDM_MAINTENANCE_ORDER,
      'Maintenance Order',
      'IDI BOM linked to at least one maintenance order',
      740
    ),
    buildRelationship(IDI_BOM, CDM_LOCATION, 'Location', 'IDI BOM linked to facility location', 520),
  ],
  [viewKey(RMDM_EQUIPMENT)]: [
    buildRelationship(RMDM_EQUIPMENT, CDM_ASSET, 'Asset', 'Equipment linked to asset hierarchy', 360),
    buildRelationship(
      RMDM_EQUIPMENT,
      IDM_MAINTENANCE_ORDER,
      'Maintenance Order',
      'Equipment linked to maintenance orders',
      290
    ),
    buildRelationship(RMDM_EQUIPMENT, CDM_LOCATION, 'Location', 'Equipment linked to location', 180),
  ],
};

export function getViewsForDataModel(space: string): readonly DiscoveredView[] {
  return MOCK_VIEWS_BY_SPACE[space] ?? [];
}

export function getRelationshipsForView(sourceView: ViewRef): readonly DiscoveredRelationship[] {
  return MOCK_RELATIONSHIPS_BY_SOURCE[viewKey(sourceView)] ?? [];
}
