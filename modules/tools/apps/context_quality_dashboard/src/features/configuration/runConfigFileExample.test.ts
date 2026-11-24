import { describe, expect, it, vi } from 'vitest';

import { parseRunConfigFile } from './runConfigFile';
import {
  downloadRunConfigExampleFile,
  RUN_CONFIG_FILE_EXAMPLE_FILENAME,
  RUN_CONFIG_FILE_EXAMPLE_YAML,
} from './runConfigFileExample';

describe('runConfigFileExample', () => {
  it('parses as a valid configuration file', () => {
    expect(parseRunConfigFile(RUN_CONFIG_FILE_EXAMPLE_YAML)).toEqual({
      ok: true,
      models: [
        { model: 'cdf_cdm:CogniteCore:v1', links: ['CogniteAsset.type', 'CogniteEquipment.equipmentType'] },
        { model: 'my_space:MyModel:v2', links: ['MyView.owner'] },
      ],
    });
  });

  it('downloads the example with the expected filename', () => {
    const click = vi.fn();
    const remove = vi.fn();
    const anchor = { href: '', download: '', rel: '', click, remove } as unknown as HTMLAnchorElement;
    const createElement = vi.spyOn(document, 'createElement').mockReturnValue(anchor);
    const appendChild = vi.spyOn(document.body, 'appendChild').mockImplementation(() => anchor);
    const createObjectURL = vi.spyOn(URL, 'createObjectURL').mockReturnValue('blob:example');
    const revokeObjectURL = vi.spyOn(URL, 'revokeObjectURL').mockImplementation(() => undefined);

    downloadRunConfigExampleFile();

    expect(createObjectURL).toHaveBeenCalled();
    expect(anchor.download).toBe(RUN_CONFIG_FILE_EXAMPLE_FILENAME);
    expect(click).toHaveBeenCalledTimes(1);
    expect(remove).toHaveBeenCalledTimes(1);
    expect(revokeObjectURL).toHaveBeenCalledWith('blob:example');

    createElement.mockRestore();
    appendChild.mockRestore();
    createObjectURL.mockRestore();
    revokeObjectURL.mockRestore();
  });
});
