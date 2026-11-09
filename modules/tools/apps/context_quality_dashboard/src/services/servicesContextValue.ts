import { createContext } from 'react';

import type { AppServices } from './createServices';

/** There is deliberately no default: services are built once from the deployment configuration and provided. */
export const ServicesContext = createContext<AppServices | null>(null);
