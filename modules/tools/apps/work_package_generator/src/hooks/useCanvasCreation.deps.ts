import {
	consumeCanvasCleanupIncomplete,
	createCanvasFromTemplateAndSelections,
	generateCanvasLink,
	updateCanvasFromTemplateAndSelections,
} from "../api";
import { fusionOrganizationFromRuntime, getAppConfig } from "../app-config";
import { useDefaultWorkPackageStore } from "../contexts/WorkPackageStoreContext";
import { useCdfClientInfo } from "./useClientInfo";

function readAncestorOrigins(): string[] {
	const location = window.location as Location & {
		ancestorOrigins?: ArrayLike<string>;
	};
	const origins = location.ancestorOrigins;
	if (!origins) return [];
	return Array.from(origins);
}

export const defaultCanvasCreationDeps = {
	consumeCanvasCleanupIncomplete,
	createCanvasFromTemplateAndSelections,
	updateCanvasFromTemplateAndSelections,
	generateCanvasLink,
	useWorkPackageStore: useDefaultWorkPackageStore,
	useCdfClientInfo,
	getOrg: () => {
		const config = getAppConfig();
		const organization =
			fusionOrganizationFromRuntime(
				{
					hostname: window.location.hostname,
					ancestorOrigins: readAncestorOrigins(),
					referrer: document.referrer,
				},
				config.fusion.hostSuffix
			) ?? config.fusion.fallbackOrganization;
		if (!organization) {
			throw new Error(
				"Fusion organization is not available. Open the app from Fusion, or set fusion.fallbackOrganization on the config node."
			);
		}
		return organization;
	},
};
