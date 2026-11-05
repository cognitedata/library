import "@testing-library/jest-dom/vitest";
import { DEFAULT_NODE_PROPERTIES } from "@/__mocks__/defaultNodeProperties";
import { parseAppConfig, setAppConfig } from "@/config/appConfig";

setAppConfig(parseAppConfig(DEFAULT_NODE_PROPERTIES));
