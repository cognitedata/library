import "@testing-library/jest-dom/vitest";
import rawAppJson from "./src/__mocks__/app-config.fixture.json";
import { installAppConfig, loadAppConfig } from "./src/app-config/load";

installAppConfig(loadAppConfig(rawAppJson));
