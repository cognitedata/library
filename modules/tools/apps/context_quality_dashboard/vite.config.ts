import path from 'node:path';
import { fileURLToPath } from 'node:url';

import { fusionOpenPlugin, manifestCspPlugin, mkcertPlugin } from '@cognite/app-sdk/vite';
import tailwindcss from '@tailwindcss/vite';
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

const dirname = path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  base: './',
  // mkcertPlugin (from the SDK) also answers Fusion's /__probe request; without it Fusion reports "Connection timed out".
  plugins: [manifestCspPlugin(), tailwindcss(), react(), mkcertPlugin(), fusionOpenPlugin()],
  resolve: {
    alias: {
      '@': path.resolve(dirname, './src'),
    },
  },
  server: {
    // Pin the port Fusion embeds in the dev URL (`…/development/<externalId>/<port>`).
    // Without strictPort, Vite picks the next free port and an old Fusion tab times out.
    port: 3004,
    strictPort: true,
  },
  worker: {
    format: 'es',
  },
});
