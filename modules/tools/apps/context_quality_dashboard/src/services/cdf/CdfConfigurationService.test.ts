import { describe, expect, it } from 'vitest';

import { createSharedConfigState } from '../sharedConfigState';

import { CdfConfigurationService } from './CdfConfigurationService';
import { NEUTRAL_CONFIG } from './neutralConfig';

function service() {
  return new CdfConfigurationService({ configState: createSharedConfigState(NEUTRAL_CONFIG) });
}

describe(CdfConfigurationService.name, () => {
  it('starts from settings that describe no project', async () => {
    const config = await service().getConfig();

    expect(config.ownerMappings).toEqual([]);
    expect(config.exclusionRules).toEqual([]);
    expect(config.notificationRecipients).toEqual([]);
    expect(config.viewBindings).toEqual([]);
    expect(config.scheduleLabel).toBe('');
    expect(config.tieringEnabled).toBe(false);
  });

  it('applies the tiering view toggle for the session', async () => {
    const configuration = service();

    await expect(configuration.updateConfig({ tieringEnabled: true })).resolves.toMatchObject({ tieringEnabled: true });
    await expect(configuration.getConfig()).resolves.toMatchObject({ tieringEnabled: true });
  });

  it.each([
    [{ maintenanceViewsAvailable: true }],
    [{ ownerMappings: [] }],
    [{ notificationRecipients: ['someone@example.test'] }],
    [{ tieringEnabled: false, scheduleLabel: 'Daily' }],
  ])('rejects the change %j instead of accepting and losing it', async (patch) => {
    const configuration = service();

    await expect(configuration.updateConfig(patch)).rejects.toMatchObject({ kind: 'unsupported-operation' });
    await expect(configuration.getConfig()).resolves.toEqual(NEUTRAL_CONFIG);
  });

  it('has no quality rules and refuses to create, change, delete or preview them', async () => {
    const configuration = service();
    const draft = { name: 'x' } as never;

    await expect(configuration.getQualityRules()).resolves.toEqual([]);
    await expect(configuration.createQualityRule(draft)).rejects.toMatchObject({ kind: 'unsupported-operation' });
    await expect(configuration.updateQualityRule(draft)).rejects.toMatchObject({ kind: 'unsupported-operation' });
    await expect(configuration.deleteQualityRule('r1')).rejects.toMatchObject({ kind: 'unsupported-operation' });
    await expect(configuration.previewQualityRule(draft)).rejects.toMatchObject({ kind: 'unsupported-operation' });
    await expect(configuration.previewExclusion({ kind: 'file-extension', pattern: '.png' })).rejects.toMatchObject({
      kind: 'unsupported-operation',
    });
  });
});
