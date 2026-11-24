import type { HostAppAPI } from '@cognite/app-sdk';
import { vi } from 'vitest';

export function makeMockHostApi(): HostAppAPI {
  return {
    getProject: vi.fn(() => Promise.resolve('test-project')),
    getBaseUrl: vi.fn(() => Promise.resolve('https://cognite.test')),
    getAccessToken: vi.fn(() => Promise.resolve('test-token')),
    getAppId: vi.fn(() => Promise.resolve('test-app-id')),
    syncInternalState: vi.fn(() => Promise.resolve(true)),
    navigateInternal: vi.fn(() => Promise.resolve(true)),
    navigateExternal: vi.fn(() => Promise.resolve(true)),
    registerAgentServer: vi.fn(() => Promise.resolve()),
    unregisterAgentServer: vi.fn(() => Promise.resolve()),
    sendAgentLayoutMode: vi.fn(() => Promise.resolve()),
    sendAgentMessage: vi.fn(() => Promise.resolve()),
    sendAgentTheme: vi.fn(() => Promise.resolve()),
    setActiveAgent: vi.fn(() => Promise.resolve()),
    setHideShell: vi.fn(() => Promise.resolve()),
  };
}
