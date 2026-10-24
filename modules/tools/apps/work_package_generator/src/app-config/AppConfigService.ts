import {
	type CogniteClient,
	CogniteError,
	type DataModel,
	type ViewReference,
} from "@cognite/sdk";
import { WPG_CONFIG_LOCATION } from "./config-location";
import { AppConfigError } from "./errors";
import { loadAppConfig } from "./load";
import type { AppConfig } from "./types";

export type AppConfigLoadErrorKind =
	| "schemaMissing"
	| "notFound"
	| "forbidden"
	| "invalid"
	| "unsupportedVersion"
	| "network";

export class AppConfigLoadError extends Error {
	readonly kind: AppConfigLoadErrorKind;
	readonly issues: readonly string[];

	constructor(
		kind: AppConfigLoadErrorKind,
		message: string,
		issues: readonly string[] = []
	) {
		super(message);
		this.name = "AppConfigLoadError";
		this.kind = kind;
		this.issues = issues;
	}
}

export interface AppConfigService {
	load(): Promise<AppConfig>;
}

export function createDmsAppConfigService(
	client: CogniteClient
): AppConfigService {
	return new DmsAppConfigService(client);
}

class DmsAppConfigService implements AppConfigService {
	constructor(private readonly client: CogniteClient) {}

	async load(): Promise<AppConfig> {
		const view = await this.resolveView();
		let response: Awaited<ReturnType<CogniteClient["instances"]["retrieve"]>>;
		try {
			response = await this.client.instances.retrieve({
				items: [
					{
						instanceType: "node",
						space: WPG_CONFIG_LOCATION.instanceSpace,
						externalId: WPG_CONFIG_LOCATION.nodeExternalId,
					},
				],
				sources: [{ source: view }],
			});
		} catch (error) {
			throw toLoadError(error);
		}

		const item = response.items[0];
		if (item?.instanceType !== "node") {
			throw new AppConfigLoadError(
				"notFound",
				`Config node ${WPG_CONFIG_LOCATION.nodeExternalId} was not found in space ${WPG_CONFIG_LOCATION.instanceSpace}.`
			);
		}

		const properties =
			item.properties?.[view.space]?.[`${view.externalId}/${view.version}`];
		if (!properties) {
			throw new AppConfigLoadError(
				"invalid",
				"Config node did not include WPGConfig properties.",
				["WPGConfig: properties missing"]
			);
		}

		try {
			return loadAppConfig(properties);
		} catch (error) {
			if (error instanceof AppConfigLoadError) throw error;
			if (error instanceof AppConfigError) {
				const unsupported = error.issues.some((issue) =>
					issue.includes("unsupported version")
				);
				throw new AppConfigLoadError(
					unsupported ? "unsupportedVersion" : "invalid",
					error.message,
					error.issues
				);
			}
			throw toLoadError(error);
		}
	}

	private async resolveView(): Promise<ViewReference> {
		let models: DataModel[];
		try {
			models = await this.client.dataModels
				.list({
					space: WPG_CONFIG_LOCATION.schemaSpace,
					allVersions: false,
					limit: 1000,
					inlineViews: true,
				})
				.autoPagingToArray({ limit: 100 });
		} catch (error) {
			throw toLoadError(error);
		}

		const model = latestModel(models);
		if (!model) {
			throw new AppConfigLoadError(
				"schemaMissing",
				`Data model ${WPG_CONFIG_LOCATION.dataModelExternalId} was not found in space ${WPG_CONFIG_LOCATION.schemaSpace}.`
			);
		}

		const view = findConfigView(model);
		if (!view) {
			throw new AppConfigLoadError(
				"schemaMissing",
				`Data model ${WPG_CONFIG_LOCATION.dataModelExternalId} has no ${WPG_CONFIG_LOCATION.viewExternalId} view.`
			);
		}
		return view;
	}
}

function latestModel(models: DataModel[]): DataModel | undefined {
	const matches = models.filter(
		(model) => model.externalId === WPG_CONFIG_LOCATION.dataModelExternalId
	);
	return matches
		.sort((left, right) => compareVersions(left.version, right.version))
		.at(-1);
}

function compareVersions(left: string, right: string): number {
	const leftNumber = numberedVersion(left);
	const rightNumber = numberedVersion(right);
	if (leftNumber !== null && rightNumber !== null)
		return leftNumber - rightNumber;
	return left.localeCompare(right);
}

function numberedVersion(version: string): number | null {
	const match = /^v(\d+)$/.exec(version);
	if (!match) return null;
	return Number(match[1]);
}

function findConfigView(model: DataModel): ViewReference | undefined {
	for (const view of model.views ?? []) {
		if (view.externalId !== WPG_CONFIG_LOCATION.viewExternalId) continue;
		if (!("version" in view) || typeof view.version !== "string") continue;
		return {
			type: "view",
			space: view.space,
			externalId: view.externalId,
			version: view.version,
		};
	}
	return undefined;
}

function toLoadError(error: unknown): AppConfigLoadError {
	if (error instanceof AppConfigLoadError) return error;
	const status = error instanceof CogniteError ? error.status : undefined;
	if (status === 403) {
		return new AppConfigLoadError(
			"forbidden",
			"You do not have access to read the Work Package Generator config."
		);
	}
	if (status === 404) {
		return new AppConfigLoadError(
			"notFound",
			`Config node ${WPG_CONFIG_LOCATION.nodeExternalId} was not found in space ${WPG_CONFIG_LOCATION.instanceSpace}.`
		);
	}
	const detail = error instanceof Error ? error.message : "Unknown error";
	return new AppConfigLoadError("network", detail);
}
