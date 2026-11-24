import { describe, expect, it } from 'vitest';

import type { RunConfigurationDto } from '../../types';

import {
  getRunCustomConfigDisabledReasonKey,
  hasPendingRunConfig,
  MAX_CONFIG_FILE_CHARS,
  parseRunConfigFile,
  runConfigModelsEqual,
  runConfigModelsFromLatestRun,
  toRunPayload,
  type RunConfigModel,
} from './runConfigFile';
import { RUN_CONFIG_FILE_EXAMPLE_YAML } from './runConfigFileExample';

const CORE = 'cdf_cdm:CogniteCore:v1';

describe(parseRunConfigFile.name, () => {
  it('reads a YAML file: a list of data models, each with its View.property links', () => {
    expect(parseRunConfigFile(RUN_CONFIG_FILE_EXAMPLE_YAML)).toEqual({
      ok: true,
      models: [
        { model: CORE, links: ['CogniteAsset.type', 'CogniteEquipment.equipmentType'] },
        { model: 'my_space:MyModel:v2', links: ['MyView.owner'] },
      ],
    });
  });

  it('reads the same structure as JSON', () => {
    const json = JSON.stringify([{ model: CORE, links: ['CogniteAsset.type'] }]);

    expect(parseRunConfigFile(json)).toEqual({ ok: true, models: [{ model: CORE, links: ['CogniteAsset.type'] }] });
  });

  it('trims the text it reads', () => {
    const file = `- model: "  ${CORE} "\n  links: ["  CogniteAsset.type "]`;

    expect(parseRunConfigFile(file)).toEqual({ ok: true, models: [{ model: CORE, links: ['CogniteAsset.type'] }] });
  });

  it.each([
    ['', 'empty'],
    ['   \n# only a comment\n', 'empty'],
    ['[]', 'empty'],
    ['model: x', 'not-a-list'],
    ['- just text', 'entry-not-a-mapping'],
    ['- links: [A.b]', 'model-missing'],
    ['- model: cdf_cdm:CogniteCore\n  links: [A.b]', 'model-format'],
    ['- model: a::v1\n  links: [A.b]', 'model-format'],
    [`- model: ${CORE}`, 'links-missing'],
    [`- model: ${CORE}\n  links: []`, 'links-missing'],
    [`- model: ${CORE}\n  links: [CogniteAsset]`, 'link-format'],
    [`- model: ${CORE}\n  links: [A.b.c]`, 'link-format'],
    [`- model: ${CORE}\n  links: [.type]`, 'link-format'],
    [`- model: ${CORE}\n  links: [3]`, 'link-format'],
    [`- model: ${CORE}\n  links: [A.b, A.b]`, 'link-duplicate'],
  ])('rejects %j with the problem %s', (text, code) => {
    const result = parseRunConfigFile(text);

    expect(result.ok).toBe(false);
    expect(result.ok ? [] : result.errors.map((error) => error.code)).toContain(code);
  });

  it('names the entry that is wrong, so the file can be fixed', () => {
    const result = parseRunConfigFile(`- model: ${CORE}\n  links: [A.b]\n- model: bad\n  links: [C.d]`);

    expect(result).toMatchObject({ ok: false, errors: [{ code: 'model-format', subject: 'bad' }] });
  });

  it('reports every problem, not only the first', () => {
    const result = parseRunConfigFile('- model: bad\n  links: [nope]');

    expect(result.ok ? [] : result.errors.map((error) => error.code)).toEqual(['model-format', 'link-format']);
  });

  it('rejects text that is neither YAML nor JSON', () => {
    const result = parseRunConfigFile('- model: [unclosed');

    expect(result).toMatchObject({ ok: false, errors: [{ code: 'unreadable' }] });
  });

  it('rejects a file that is too large to send with a run', () => {
    const result = parseRunConfigFile('#'.repeat(MAX_CONFIG_FILE_CHARS + 1));

    expect(result).toMatchObject({ ok: false, errors: [{ code: 'too-large' }] });
  });
});

describe(toRunPayload.name, () => {
  it('is the extraLinks the function takes in its payload', () => {
    expect(toRunPayload([{ model: CORE, links: ['CogniteAsset.type'] }])).toEqual({
      extraLinks: [{ model: CORE, links: ['CogniteAsset.type'] }],
    });
  });
});

describe(runConfigModelsFromLatestRun.name, () => {
  it('flattens views and properties into View.property links', () => {
    const configuration: RunConfigurationDto = {
      runTimestamp: '2026-10-15T17:45:27.000Z',
      models: [
        {
          model: CORE,
          views: [
            { view: 'CogniteAsset', properties: ['type', 'assetClass'] },
            { view: 'CogniteEquipment', properties: ['equipmentType'] },
          ],
        },
      ],
    };

    expect(runConfigModelsFromLatestRun(configuration)).toEqual([
      {
        model: CORE,
        links: ['CogniteAsset.type', 'CogniteAsset.assetClass', 'CogniteEquipment.equipmentType'],
      },
    ]);
  });
});

describe(runConfigModelsEqual.name, () => {
  const a: RunConfigModel[] = [
    { model: CORE, links: ['CogniteAsset.type', 'CogniteEquipment.equipmentType'] },
    { model: 'my_space:MyModel:v2', links: ['MyView.owner'] },
  ];

  it('treats the same models and links as equal regardless of order', () => {
    const shuffled: RunConfigModel[] = [
      { model: 'my_space:MyModel:v2', links: ['MyView.owner'] },
      { model: CORE, links: ['CogniteEquipment.equipmentType', 'CogniteAsset.type'] },
    ];

    expect(runConfigModelsEqual(a, shuffled)).toBe(true);
  });

  it('is false when a link differs', () => {
    expect(
      runConfigModelsEqual(a, [
        { model: CORE, links: ['CogniteAsset.type'] },
        { model: 'my_space:MyModel:v2', links: ['MyView.owner'] },
      ])
    ).toBe(false);
  });
});

describe(hasPendingRunConfig.name, () => {
  const models: RunConfigModel[] = [{ model: CORE, links: ['CogniteAsset.type'] }];

  it('is false while the latest run is still loading', () => {
    expect(hasPendingRunConfig(models, undefined)).toBe(false);
  });

  it('is true when no run has completed yet', () => {
    expect(hasPendingRunConfig(models, null)).toBe(true);
  });

  it('is false when the draft matches the latest run', () => {
    const latest: RunConfigurationDto = {
      runTimestamp: '2026-10-15T17:45:27.000Z',
      models: [{ model: CORE, views: [{ view: 'CogniteAsset', properties: ['type'] }] }],
    };

    expect(hasPendingRunConfig(models, latest)).toBe(false);
  });

  it('is true when the draft differs from the latest run', () => {
    const latest: RunConfigurationDto = {
      runTimestamp: '2026-10-15T17:45:27.000Z',
      models: [{ model: CORE, views: [{ view: 'CogniteAsset', properties: ['type'] }] }],
    };

    expect(hasPendingRunConfig([{ model: CORE, links: ['CogniteAsset.assetClass'] }], latest)).toBe(true);
  });
});

describe(getRunCustomConfigDisabledReasonKey.name, () => {
  const models: RunConfigModel[] = [{ model: CORE, links: ['CogniteAsset.type'] }];

  it('returns null when the run would be enabled', () => {
    expect(
      getRunCustomConfigDisabledReasonKey({
        models,
        configChangedSinceLatestRun: true,
        busy: false,
        runInProgress: false,
        otherRunInProgress: false,
        latestRunConfiguration: null,
      })
    ).toBe(null);
  });

  it('returns matchesLatest when the draft equals the latest run', () => {
    expect(
      getRunCustomConfigDisabledReasonKey({
        models,
        configChangedSinceLatestRun: false,
        busy: false,
        runInProgress: false,
        otherRunInProgress: false,
        latestRunConfiguration: {
          runTimestamp: '2026-10-15T17:45:27.000Z',
          models: [{ model: CORE, views: [{ view: 'CogniteAsset', properties: ['type'] }] }],
        },
      })
    ).toBe('configuration.run.disabled.matchesLatest');
  });

  it('returns loadingLatest while the latest run is still loading', () => {
    expect(
      getRunCustomConfigDisabledReasonKey({
        models,
        configChangedSinceLatestRun: false,
        busy: false,
        runInProgress: false,
        otherRunInProgress: false,
        latestRunConfiguration: undefined,
      })
    ).toBe('configuration.run.disabled.loadingLatest');
  });

  it('returns busy when a run is already in progress', () => {
    expect(
      getRunCustomConfigDisabledReasonKey({
        models,
        configChangedSinceLatestRun: true,
        busy: true,
        runInProgress: true,
        otherRunInProgress: false,
        latestRunConfiguration: null,
      })
    ).toBe('configuration.run.disabled.busy');
  });
});
