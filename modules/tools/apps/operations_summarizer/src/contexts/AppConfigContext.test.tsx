import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import type { ReactNode } from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { DEFAULT_NODE_PROPERTIES } from "@/__mocks__/defaultNodeProperties";
import {
  clearAppConfig,
  getAppConfig,
  parseAppConfig,
} from "@/config/appConfig";
import {
  AppConfigProvider,
  type AppConfigProviderProps,
} from "@/contexts/AppConfigContext";
import { CdfClientProvider } from "@/contexts/CdfClientContext";
import type { AppConfigService } from "@/services/appConfigService";

// Host SDK reads the Fusion shell client. Tests stub useCogniteSdk because there is no shell in jsdom.
// Stub the Fusion host so unit tests do not boot the real App SDK.
vi.mock("@cognite/app-sdk/react", () => ({
  useCogniteSdk: () => ({ project: "test-project" }),
}));

describe(AppConfigProvider.name, () => {
  let service: AppConfigService;

  beforeEach(() => {
    clearAppConfig();
    service = {
      load: vi.fn(() =>
        Promise.resolve(parseAppConfig(DEFAULT_NODE_PROPERTIES)),
      ),
    };
  });

  it("shows a loading state while the node is loading", () => {
    service = {
      load: () => new Promise(() => undefined),
    };
    renderProvider({ serviceFactory: () => service });
    expect(screen.getByLabelText("Loading configuration")).toBeInTheDocument();
    expect(screen.queryByText("ready")).not.toBeInTheDocument();
  });

  it("shows a blocking error that names the node, schema space, and required access", async () => {
    service = {
      load: () => Promise.reject(new Error("node was not found")),
    };
    renderProvider({ serviceFactory: () => service });

    expect(await screen.findByRole("alert")).toBeInTheDocument();
    expect(screen.getByRole("alert")).toHaveTextContent(
      "inst_ops_summarizer_app_config/ops_summarizer_config",
    );
    expect(screen.getByRole("alert")).toHaveTextContent(
      "dm_sol_ops_summarizer_app",
    );
    expect(screen.getByRole("alert")).toHaveTextContent(
      "inst_ops_summarizer_app_config",
    );
    expect(screen.getByRole("alert")).toHaveTextContent("dataModels:read");
    expect(screen.getByRole("alert")).toHaveTextContent(
      "dataModelInstances:read",
    );
    expect(screen.getByRole("alert")).toHaveTextContent("node was not found");
    expect(screen.queryByText("ready")).not.toBeInTheDocument();
  });

  it("renders children after the node loads", async () => {
    const loaded = parseAppConfig({
      ...DEFAULT_NODE_PROPERTIES,
      companyName: "Acme",
    });
    service = { load: () => Promise.resolve(loaded) };
    renderProvider({ serviceFactory: () => service });

    expect(await screen.findByText("ready")).toBeInTheDocument();
    expect(getAppConfig().companyName).toBe("Acme");
  });
});

function renderProvider(props: Pick<AppConfigProviderProps, "serviceFactory">) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  function Wrapper({ children }: { children: ReactNode }) {
    return (
      <QueryClientProvider client={queryClient}>
        <CdfClientProvider>
          <AppConfigProvider serviceFactory={props.serviceFactory}>
            {children}
          </AppConfigProvider>
        </CdfClientProvider>
      </QueryClientProvider>
    );
  }
  return render(<p>ready</p>, { wrapper: Wrapper });
}
