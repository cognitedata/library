import type { HostAppAPI } from '@cognite/app-sdk';

/**
 * Services are created once at module load, before the Fusion host handshake finishes.
 * The CDF-backed services read the host API lazily through this holder; App sets it after connecting.
 */
export const hostApiHolder: { current: HostAppAPI | null } = { current: null };
