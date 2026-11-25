import { QueryClientProvider } from "@tanstack/react-query";
import { act, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { createTestQueryClient } from "@/test-utils/wrappers";
import { CdfClientProvider } from "./CdfClientContext";
import {
	OperationsProvider,
	useOperations,
} from "./OperationsSummarizerContext";

// Host SDK reads the Fusion shell client. Tests stub useCogniteSdk because there is no shell in jsdom.
// Stub the Fusion host so unit tests do not boot the real App SDK.
vi.mock("@cognite/app-sdk/react", () => ({
	useCogniteSdk: () => null,
}));

const queryClient = createTestQueryClient();

// AgentBootstrap creates the default agent through a React Query mutation.
function WithCdf({ children }: { children: React.ReactNode }) {
	return (
		<QueryClientProvider client={queryClient}>
			<CdfClientProvider>{children}</CdfClientProvider>
		</QueryClientProvider>
	);
}

function TestConsumer() {
	const { state, setSelectedAgent, setSummaryPeriod, toggleSummaryContent } =
		useOperations();
	return (
		<div>
			<span data-testid="agent">{state.selectedAgent ?? "none"}</span>
			<span data-testid="assets">{String(state.summaryContents.assets)}</span>
			<button type="button" onClick={() => setSelectedAgent("agent-1")}>
				Set Agent
			</button>
			<button
				type="button"
				onClick={() =>
					setSummaryPeriod(new Date("2024-01-01"), new Date("2024-01-31"))
				}
			>
				Set Period
			</button>
			<button type="button" onClick={() => toggleSummaryContent("assets")}>
				Toggle Assets
			</button>
		</div>
	);
}

describe("OperationsSummarizerContext", () => {
	it("provides initial state", () => {
		render(
			<WithCdf>
				<OperationsProvider>
					<TestConsumer />
				</OperationsProvider>
			</WithCdf>
		);
		expect(screen.getByTestId("agent").textContent).toBe("none");
		expect(screen.getByTestId("assets").textContent).toBe("true");
	});

	it("updates selectedAgent when setSelectedAgent is called", () => {
		render(
			<WithCdf>
				<OperationsProvider>
					<TestConsumer />
				</OperationsProvider>
			</WithCdf>
		);
		act(() => {
			screen.getByText("Set Agent").click();
		});
		expect(screen.getByTestId("agent").textContent).toBe("agent-1");
	});

	it("calls setSummaryPeriod without throwing", () => {
		render(
			<WithCdf>
				<OperationsProvider>
					<TestConsumer />
				</OperationsProvider>
			</WithCdf>
		);
		expect(() =>
			act(() => {
				screen.getByText("Set Period").click();
			})
		).not.toThrow();
	});

	it("toggles summary content when toggleSummaryContent is called", () => {
		render(
			<WithCdf>
				<OperationsProvider>
					<TestConsumer />
				</OperationsProvider>
			</WithCdf>
		);
		expect(screen.getByTestId("assets").textContent).toBe("true");
		act(() => {
			screen.getByText("Toggle Assets").click();
		});
		expect(screen.getByTestId("assets").textContent).toBe("false");
		act(() => {
			screen.getByText("Toggle Assets").click();
		});
		expect(screen.getByTestId("assets").textContent).toBe("true");
	});
});
