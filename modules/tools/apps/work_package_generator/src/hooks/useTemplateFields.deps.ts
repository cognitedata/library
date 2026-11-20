import { getTemplateCanvasInstances } from "../api";
import { useDefaultWorkPackageStore } from "../contexts/WorkPackageStoreContext";
import { useCdfClientInfo } from "./useClientInfo";

export const defaultTemplateFieldsDeps = {
	getTemplateCanvasInstances: (
		...args: Parameters<typeof getTemplateCanvasInstances>
	) => getTemplateCanvasInstances(...args),
	useWorkPackageStore: useDefaultWorkPackageStore,
	useCdfClientInfo,
};
