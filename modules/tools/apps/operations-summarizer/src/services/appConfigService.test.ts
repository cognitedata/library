import { assert, describe, expect, it, vi } from "vitest";
import { DEFAULT_NODE_PROPERTIES } from "@/__mocks__/defaultNodeProperties";
import { AppConfigError } from "@/config/appConfig";
import {
  CONFIG_DATA_MODEL_EXTERNAL_ID,
  CONFIG_INSTANCE_SPACE,
  CONFIG_NODE_EXTERNAL_ID,
  CONFIG_SCHEMA_SPACE,
  CONFIG_VIEW_EXTERNAL_ID,
} from "@/config/configLocation";
import {
  type AppConfigCdfClient,
  CdfAppConfigService,
} from "@/services/appConfigService";

const VIEW_VERSION = "v1";

describe(CdfAppConfigService.name, () => {
  it("lists the config data model and retrieves the node with that view", async () => {
    const client = clientReturning(DEFAULT_NODE_PROPERTIES);

    const config = await new CdfAppConfigService(client).load();

    expect(client.dataModels.list).toHaveBeenCalledWith({
      space: CONFIG_SCHEMA_SPACE,
      allVersions: false,
      limit: 1000,
    });
    expect(client.instances.retrieve).toHaveBeenCalledWith({
      items: [
        {
          instanceType: "node",
          space: CONFIG_INSTANCE_SPACE,
          externalId: CONFIG_NODE_EXTERNAL_ID,
        },
      ],
      sources: [
        {
          source: {
            type: "view",
            space: CONFIG_SCHEMA_SPACE,
            externalId: CONFIG_VIEW_EXTERNAL_ID,
            version: VIEW_VERSION,
          },
        },
      ],
    });
    expect(config.schemaVersion).toBe(1);
    expect(config.spaces.summaries).toBe("ops_summaries");
    expect(config.agent.defaultLanguageModel).toBe("cognite/balanced");
  });

  it("accepts a list response shaped as { items }", async () => {
    const client = clientReturning(DEFAULT_NODE_PROPERTIES);
    client.dataModels.list.mockResolvedValue({
      items: [
        {
          externalId: CONFIG_DATA_MODEL_EXTERNAL_ID,
          views: [{ externalId: CONFIG_VIEW_EXTERNAL_ID, version: "v3" }],
        },
      ],
    });
    client.instances.retrieve.mockResolvedValue({
      items: [
        {
          properties: {
            [CONFIG_SCHEMA_SPACE]: {
              [`${CONFIG_VIEW_EXTERNAL_ID}/v3`]: DEFAULT_NODE_PROPERTIES,
            },
          },
        },
      ],
    });

    await new CdfAppConfigService(client).load();

    expect(client.instances.retrieve).toHaveBeenCalledWith(
      expect.objectContaining({
        sources: [
          expect.objectContaining({
            source: expect.objectContaining({ version: "v3" }),
          }),
        ],
      }),
    );
  });

  it("throws when the data model is missing", async () => {
    const client = clientReturning(DEFAULT_NODE_PROPERTIES);
    client.dataModels.list.mockResolvedValue([]);

    await expect(new CdfAppConfigService(client).load()).rejects.toThrow(
      AppConfigError,
    );
    await expect(new CdfAppConfigService(client).load()).rejects.toThrow(
      /OpsSummarizerConfig_SOL/,
    );
  });

  it("throws when the node is missing", async () => {
    const client = clientReturning(DEFAULT_NODE_PROPERTIES);
    client.instances.retrieve.mockResolvedValue({ items: [] });

    await expect(new CdfAppConfigService(client).load()).rejects.toThrow(
      /ops_summarizer_config/,
    );
  });

  it("throws when the node properties omit the view key", async () => {
    const client = clientReturning(DEFAULT_NODE_PROPERTIES);
    client.instances.retrieve.mockResolvedValue({
      items: [{ properties: { [CONFIG_SCHEMA_SPACE]: {} } }],
    });

    await expect(new CdfAppConfigService(client).load()).rejects.toThrow(
      /dataModelInstances:read/,
    );
  });

  it("throws when listing data models fails", async () => {
    const client = clientReturning(DEFAULT_NODE_PROPERTIES);
    client.dataModels.list.mockRejectedValue({ status: 403, message: "no" });

    await expect(new CdfAppConfigService(client).load()).rejects.toThrow(
      /status 403/,
    );
  });

  it("throws when an unsupported schemaVersion is stored on the node", async () => {
    const client = clientReturning({
      ...DEFAULT_NODE_PROPERTIES,
      schemaVersion: 2,
    });

    await expect(new CdfAppConfigService(client).load()).rejects.toThrow(
      /schemaVersion 2/,
    );
  });
});

function clientReturning(
  properties: Record<string, unknown>,
): AppConfigCdfClient & {
  dataModels: {
    list: ReturnType<typeof vi.fn>;
  };
  instances: {
    retrieve: ReturnType<typeof vi.fn>;
  };
} {
  const list = vi.fn(() =>
    Promise.resolve([
      {
        externalId: CONFIG_DATA_MODEL_EXTERNAL_ID,
        views: [{ externalId: CONFIG_VIEW_EXTERNAL_ID, version: VIEW_VERSION }],
      },
    ]),
  );
  const retrieve = vi.fn(() =>
    Promise.resolve({
      items: [
        {
          properties: {
            [CONFIG_SCHEMA_SPACE]: {
              [`${CONFIG_VIEW_EXTERNAL_ID}/${VIEW_VERSION}`]: properties,
            },
          },
        },
      ],
    }),
  );
  if (list.mock === undefined) {
    assert.fail("expected vi.fn");
  }
  return {
    dataModels: { list },
    instances: { retrieve },
  };
}
