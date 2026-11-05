import { useCogniteSdk } from "@cognite/app-sdk/react";
import { render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { cogniteClientMock } from "@/test-utils/testDoubles";
import { CdfClientProvider, useCdfClient } from "./CdfClientContext";

// Host SDK reads the Fusion shell client. Tests stub useCogniteSdk because there is no shell in jsdom.
// Stub the Fusion host so unit tests do not boot the real App SDK.
vi.mock("@cognite/app-sdk/react", () => ({
  useCogniteSdk: vi.fn(),
}));

function Consumer() {
  const { sdk, isAuthLoading } = useCdfClient();
  return (
    <div>
      <span data-testid="has-sdk">{sdk ? "yes" : "no"}</span>
      <span data-testid="is-auth-loading">{String(isAuthLoading)}</span>
    </div>
  );
}

describe("CdfClientProvider", () => {
  it("reports auth loading while sdk is unavailable", () => {
    vi.mocked(useCogniteSdk).mockImplementation(() => null!);

    render(
      <CdfClientProvider>
        <Consumer />
      </CdfClientProvider>,
    );

    expect(screen.getByTestId("has-sdk")).toHaveTextContent("no");
    expect(screen.getByTestId("is-auth-loading")).toHaveTextContent("true");
  });

  it("exposes sdk and clears auth loading when sdk is ready", () => {
    vi.mocked(useCogniteSdk).mockReturnValue(
      cogniteClientMock({ project: "test-project" }),
    );

    render(
      <CdfClientProvider>
        <Consumer />
      </CdfClientProvider>,
    );

    expect(screen.getByTestId("has-sdk")).toHaveTextContent("yes");
    expect(screen.getByTestId("is-auth-loading")).toHaveTextContent("false");
  });
});

describe("useCdfClient", () => {
  it("throws outside provider", () => {
    function Bad() {
      useCdfClient();
      return null;
    }

    expect(() => render(<Bad />)).toThrow(
      /useCdfClient must be used within CdfClientProvider/,
    );
  });
});
