import { createContext, useContext } from "react";
import { useWorkPackageStore } from "@/store";

type WorkPackageSnapshot = ReturnType<typeof useWorkPackageStore.getState>;

export type WorkPackageStoreHook = {
	(): WorkPackageSnapshot;
	getState: () => WorkPackageSnapshot;
};

/**
 * Work-package store hook. Production uses the Zustand store.
 * Tests replace the provider value or a hook-level dependency with a double.
 */
export const WorkPackageStoreContext =
	createContext<WorkPackageStoreHook>(useWorkPackageStore);

/**
 * Store hook resolved from {@link WorkPackageStoreContext}.
 * `getState` reads the Zustand store for effects that must not subscribe.
 */
export const useDefaultWorkPackageStore: WorkPackageStoreHook = Object.assign(
	function useDefaultWorkPackageStore() {
		const useStore = useContext(WorkPackageStoreContext);
		return useStore();
	},
	{
		getState: () => useWorkPackageStore.getState(),
	}
);
