import { renderHook } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { cdfClientTestDouble } from "@/__mocks__/cdf-client-test-double";
import { CdfRuntimeTestProvider } from "@/contexts/CdfRuntimeContext";
import { useCdfClientInfo } from "./useClientInfo";

describe("useCdfClientInfo", () => {
	it("returns CDF runtime from context", () => {
		const client = cdfClientTestDouble({
			project: "live",
			getBaseUrl: () => "https://my.cognitedata.com",
		});

		const { result } = renderHook(() => useCdfClientInfo(), {
			wrapper: ({ children }) => (
				<CdfRuntimeTestProvider
					value={{ client, project: "live", cluster: "my" }}
				>
					{children}
				</CdfRuntimeTestProvider>
			),
		});

		expect(result.current.project).toBe("live");
		expect(result.current.cluster).toBe("my");
		expect(result.current.client).toBe(client);
	});
});
