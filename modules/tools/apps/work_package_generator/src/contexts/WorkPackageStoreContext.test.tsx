import { renderHook } from "@testing-library/react";
import type { ReactNode } from "react";
import { describe, expect, it, vi } from "vitest";
import { useWorkPackageStore } from "@/store";
import {
	useDefaultWorkPackageStore,
	WorkPackageStoreContext,
	type WorkPackageStoreHook,
} from "./WorkPackageStoreContext";

describe(useDefaultWorkPackageStore.name, () => {
	it("returns the Zustand store when no provider override is set", () => {
		const { result } = renderHook(() => useDefaultWorkPackageStore());
		expect(result.current.setError).toBe(
			useWorkPackageStore.getState().setError
		);
	});

	it("returns the store from context when a test double is provided", () => {
		const setError = vi.fn<(error: string | null) => void>();
		const double: WorkPackageStoreHook = Object.assign(
			() => ({
				...useWorkPackageStore.getState(),
				setError,
			}),
			{ getState: useWorkPackageStore.getState }
		);
		const wrapper = ({ children }: { children: ReactNode }) => (
			<WorkPackageStoreContext.Provider value={double}>
				{children}
			</WorkPackageStoreContext.Provider>
		);
		const { result } = renderHook(() => useDefaultWorkPackageStore(), {
			wrapper,
		});
		result.current.setError("x");
		expect(setError).toHaveBeenCalledWith("x");
	});
});
