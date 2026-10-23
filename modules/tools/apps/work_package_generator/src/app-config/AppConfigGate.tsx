import type { ReactNode } from "react";
import { Button, Loader } from "@/components/ui";
import { useWorkPackageStore } from "@/store";
import type { AppConfigLoadError } from "./AppConfigService";
import { WPG_CONFIG_LOCATION } from "./config-location";
import { AppConfigProvider } from "./context";
import { toCanvasConfig } from "./runtime";
import { useAppConfigLoaderViewModel } from "./useAppConfigLoaderViewModel";
import { SUPPORTED_CONFIG_SCHEMA_VERSION } from "./validate";

export function AppConfigGate({ children }: { children: ReactNode }) {
	const { isLoading, config, error, retry } = useAppConfigLoaderViewModel();

	if (isLoading || !config) {
		if (error) return <ConfigSetupScreen error={error} onRetry={retry} />;
		return <ConfigLoadingScreen />;
	}

	useWorkPackageStore.getState().setCanvasConfig(toCanvasConfig(config));

	return <AppConfigProvider value={config}>{children}</AppConfigProvider>;
}

function ConfigLoadingScreen() {
	return (
		<div
			role="status"
			className="flex h-dvh items-center justify-center"
			aria-live="polite"
			aria-label="Loading configuration"
		>
			<Loader size={20} className="text-muted-foreground" />
		</div>
	);
}

function ConfigSetupScreen({
	error,
	onRetry,
}: {
	error: AppConfigLoadError;
	onRetry: () => void;
}) {
	const copy = setupCopy(error);
	return (
		<div
			role="alert"
			className="mx-auto flex h-dvh max-w-xl flex-col items-center justify-center gap-4 p-8 text-center"
		>
			<h1 className="text-lg font-medium text-foreground">{copy.title}</h1>
			<p className="text-sm text-muted-foreground">{copy.body}</p>
			{error.issues.length > 0 ? (
				<ul className="w-full list-disc space-y-1 pl-5 text-left text-sm text-muted-foreground">
					{error.issues.map((issue) => (
						<li key={issue}>{issue}</li>
					))}
				</ul>
			) : null}
			<Button type="button" onClick={onRetry}>
				Retry
			</Button>
		</div>
	);
}

function setupCopy(error: AppConfigLoadError): { title: string; body: string } {
	const node = `${WPG_CONFIG_LOCATION.instanceSpace} / ${WPG_CONFIG_LOCATION.nodeExternalId}`;
	switch (error.kind) {
		case "schemaMissing":
			return {
				title: "Work Package Generator is not set up",
				body: `Deploy the toolkit module work_package_generator so data model ${WPG_CONFIG_LOCATION.dataModelExternalId} and view ${WPG_CONFIG_LOCATION.viewExternalId} exist in space ${WPG_CONFIG_LOCATION.schemaSpace}.`,
			};
		case "notFound":
			return {
				title: "Work Package Generator config was not found",
				body: `Deploy the toolkit module work_package_generator. The app looks for node ${node}.`,
			};
		case "forbidden":
			return {
				title: "You can't read the Work Package Generator config",
				body: `Add dataModels:read for space ${WPG_CONFIG_LOCATION.schemaSpace} and dataModelInstances:read for space ${WPG_CONFIG_LOCATION.instanceSpace} to your access group.`,
			};
		case "unsupportedVersion":
			return {
				title: "This config version is not supported",
				body: `This app reads schema version ${SUPPORTED_CONFIG_SCHEMA_VERSION}. Deploy a matching toolkit module or app.`,
			};
		case "invalid":
			return {
				title: "The Work Package Generator config is invalid",
				body: "Fix the issues below on the config node, then retry.",
			};
		case "network":
			return {
				title: "The config could not be loaded",
				body: "Check your connection and try again.",
			};
	}
}
