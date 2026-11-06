import path from 'node:path';
import { fileURLToPath } from 'node:url';

import react from '@vitejs/plugin-react';
import { configDefaults, defineConfig } from 'vitest/config';

const dirname = path.dirname(fileURLToPath(import.meta.url));

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      '@': path.resolve(dirname, './src'),
    },
  },
  test: {
    globals: true,
    environment: 'happy-dom',
    setupFiles: ['vitest.setup.ts'],
    exclude: [...configDefaults.exclude, '.claude/**', '.agents/**', '.worktrees/**', '.vendor/**'],
    coverage: {
      provider: 'v8',
      include: ['src/**/*.{ts,tsx}'],
      reporter: ['text', 'json', 'html', 'lcov'],
      exclude: [
        'node_modules/',
        'dist/',
        '.claude/',
        '.agents/',
        'vitest.setup.ts',
        '**/*.config.ts',
        '**/*.d.ts',
        '**/*.test.{ts,tsx}',
        'src/main.tsx',
      ],
    },
  },
});
