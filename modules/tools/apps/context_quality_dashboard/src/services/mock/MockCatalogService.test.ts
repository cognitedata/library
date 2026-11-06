import { describe, expect, it } from 'vitest';

import { MockCatalogService } from './MockCatalogService';

describe(MockCatalogService.name, () => {
  const service = new MockCatalogService({ latency: 0 });

  it('returns discovered data models', async () => {
    const models = await service.getDataModels();
    expect(models.some((model) => model.space === 'rmdm')).toBe(true);
  });

  it('returns views for the rmdm data model', async () => {
    const views = await service.getViews('rmdm');
    expect(views.some((view) => view.externalId === 'IDI_BOM')).toBe(true);
    expect(views.some((view) => view.externalId === 'Equipment')).toBe(true);
  });

  it('returns multiple relationship targets for IDI_BOM', async () => {
    const relationships = await service.getRelationships({ space: 'rmdm', externalId: 'IDI_BOM' });
    const targetNames = relationships.map((entry) => entry.targetDisplayName);

    expect(targetNames).toEqual(
      expect.arrayContaining(['Asset', 'Equipment', 'Maintenance Order', 'Location'])
    );
  });

  it('returns different relationships for Equipment source view', async () => {
    const relationships = await service.getRelationships({ space: 'rmdm', externalId: 'Equipment' });
    const targetNames = relationships.map((entry) => entry.targetDisplayName);

    expect(targetNames).toEqual(expect.arrayContaining(['Asset', 'Maintenance Order', 'Location']));
    expect(targetNames).not.toContain('Equipment');
  });
});
