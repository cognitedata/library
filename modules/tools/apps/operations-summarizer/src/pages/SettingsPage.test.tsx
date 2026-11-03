import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { SettingsPage } from "./SettingsPage";

const defaultLoader = () => ({
  state: { selectedAgent: null as string | null },
  currentAgent: null as {
    name: string;
    model: string | null;
    externalId: string;
    description?: string;
  } | null,
  canEditAgentConfig: true,
  canEditAgentConfigLoading: false,
  canEditAgentConfigError: false,
  refetchCanEditAgentConfig: vi.fn(),
  setAgentName: vi.fn(),
  agentName: null,
  sdk: null,
});

let loaderOverrides: Partial<ReturnType<typeof defaultLoader>> = {};

// Settings tests stub the agent loader so they do not call the agents API.
vi.mock("@/features/dashboard-agent", () => ({
  useAgentLoader: () => ({ ...defaultLoader(), ...loaderOverrides }),
}));

describe("SettingsPage", () => {
  beforeEach(() => {
    loaderOverrides = {};
  });

  it("shows a read-only agent view with no save or delete actions", () => {
    render(<SettingsPage />);
    expect(screen.getByText("Not Configured")).toBeInTheDocument();
    expect(
      screen.getByText(/updating that node and redeploying/i),
    ).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /create agent/i })).toBeNull();
    expect(screen.queryByRole("button", { name: /update agent/i })).toBeNull();
    expect(screen.queryByRole("button", { name: /delete agent/i })).toBeNull();
    expect(screen.queryByRole("button", { name: /reset/i })).toBeNull();
    expect(screen.getByLabelText("Base instructions")).toHaveAttribute(
      "readonly",
    );
  });

  it("shows the active agent without edit actions", () => {
    loaderOverrides = {
      state: { selectedAgent: "agent-123" },
      currentAgent: {
        name: "Operation Summarizer Agent",
        model: "azure/gpt-5.1",
        externalId: "proj_operation_summarizer_helper_agent",
        description: "Reads the config node",
      },
    };
    render(<SettingsPage />);
    expect(screen.getByText("Active")).toBeInTheDocument();
    expect(screen.getByText("Operation Summarizer Agent")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /delete agent/i })).toBeNull();
    expect(screen.queryByText(/Agent required/i)).toBeNull();
  });

  it("explains the write requirement when the agent is missing and the user cannot create it", () => {
    loaderOverrides = { canEditAgentConfig: false };
    render(<SettingsPage />);
    expect(screen.getByText(/agents:write/i)).toBeInTheDocument();
  });
});
