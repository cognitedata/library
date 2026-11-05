import type { CogniteClient } from "@cognite/sdk";
import { AppConfigError, parseAppConfig } from "@/config/appConfig";
import {
  CONFIG_DATA_MODEL_EXTERNAL_ID,
  CONFIG_INSTANCE_SPACE,
  CONFIG_NODE_EXTERNAL_ID,
  CONFIG_NODE_PATH,
  CONFIG_SCHEMA_SPACE,
  CONFIG_VIEW_EXTERNAL_ID,
} from "@/config/configLocation";
import type { AppConfig } from "@/types/appConfig";

export interface AppConfigService {
  load(): Promise<AppConfig>;
}

interface DataModelViewRef {
  externalId?: string;
  version?: string;
}

interface DataModelListItem {
  externalId?: string;
  views?: DataModelViewRef[];
}

interface ConfigNodeItem {
  properties?: Record<string, Record<string, unknown>>;
}

/**
 * Narrow CDF surface this service calls. `CogniteClient` satisfies it.
 * The SDK retries HTTP 429s.
 */
export interface AppConfigCdfClient {
  dataModels: {
    list: (query: {
      space?: string;
      allVersions?: boolean;
      limit?: number;
    }) => Promise<DataModelListItem[] | { items: DataModelListItem[] }>;
  };
  instances: {
    retrieve: (query: {
      items: Array<{
        instanceType: "node";
        space: string;
        externalId: string;
      }>;
      sources: Array<{
        source: {
          type: "view";
          space: string;
          externalId: string;
          version: string;
        };
      }>;
    }) => Promise<{ items: ConfigNodeItem[] }>;
  };
}

function createCdfAppConfigService(
  client: AppConfigCdfClient,
): AppConfigService {
  return new CdfAppConfigService(client);
}

export class CdfAppConfigService implements AppConfigService {
  public constructor(private readonly client: AppConfigCdfClient) {}

  public async load(): Promise<AppConfig> {
    const viewVersion = await this.readViewVersion();
    const properties = await this.readNodeProperties(viewVersion);
    return parseAppConfig(properties);
  }

  private async readViewVersion(): Promise<string> {
    let listed: DataModelListItem[] | { items: DataModelListItem[] };
    try {
      listed = await this.client.dataModels.list({
        space: CONFIG_SCHEMA_SPACE,
        allVersions: false,
        limit: 1000,
      });
    } catch (error) {
      throw requestFailed(error);
    }

    const models = Array.isArray(listed) ? listed : listed.items;
    const model = models.find(
      (item) => item.externalId === CONFIG_DATA_MODEL_EXTERNAL_ID,
    );
    const view = model?.views?.find(
      (item) => item.externalId === CONFIG_VIEW_EXTERNAL_ID,
    );
    if (!model || !view?.version) {
      throw new AppConfigError(
        `config node ${CONFIG_NODE_PATH}: data model ${CONFIG_DATA_MODEL_EXTERNAL_ID} was not found in ${CONFIG_SCHEMA_SPACE}. Grant dataModels:read on that space.`,
      );
    }
    return view.version;
  }

  private async readNodeProperties(viewVersion: string): Promise<unknown> {
    let response: { items: ConfigNodeItem[] };
    try {
      response = await this.client.instances.retrieve({
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
              version: viewVersion,
            },
          },
        ],
      });
    } catch (error) {
      throw requestFailed(error);
    }

    const node = response.items[0];
    const viewKey = `${CONFIG_VIEW_EXTERNAL_ID}/${viewVersion}`;
    const bySpace = node?.properties?.[CONFIG_SCHEMA_SPACE];
    const properties = bySpace?.[viewKey];
    if (!node || !properties) {
      throw new AppConfigError(
        `config node ${CONFIG_NODE_PATH}: node was not found in ${CONFIG_INSTANCE_SPACE}. Grant dataModelInstances:read on that space.`,
      );
    }
    return properties;
  }
}

function requestFailed(error: unknown): AppConfigError {
  const status = readStatus(error);
  const statusText =
    status === undefined ? "a request error" : `status ${status}`;
  return new AppConfigError(
    `config node ${CONFIG_NODE_PATH}: CDF request failed with ${statusText}. Grant dataModels:read on ${CONFIG_SCHEMA_SPACE} and dataModelInstances:read on ${CONFIG_INSTANCE_SPACE}.`,
    { cause: error },
  );
}

function readStatus(error: unknown): number | undefined {
  if (typeof error !== "object" || error === null) return undefined;
  const record = error as {
    status?: unknown;
    statusCode?: unknown;
    code?: unknown;
  };
  if (typeof record.status === "number") return record.status;
  if (typeof record.statusCode === "number") return record.statusCode;
  if (typeof record.code === "number") return record.code;
  return undefined;
}

/** Structural check so a real SDK client can be passed without a cast at the call site. */
export function createAppConfigServiceFromSdk(
  sdk: CogniteClient,
): AppConfigService {
  return createCdfAppConfigService(sdk);
}
