import { defineWorkersConfig } from '@cloudflare/vitest-pool-workers/config';
import path from 'path';

export default defineWorkersConfig({
  resolve: {
    alias: {
      '~': path.resolve(__dirname, './src'),
    },
  },
  test: {
    globals: true,
    poolOptions: {
      workers: {
        wrangler: { configPath: './wrangler.jsonc' },
      },
    },
    include: ['src/**/*.test.ts'],
    exclude: [
      'src/jobQueue/*.test.ts',  // Exclude tests that require Redis/BullMQ
      'src/usage.test.ts',       // Imports BullMQ which requires Node.js
    ],
    setupFiles: ['./src/test/cloudflare-setup.ts'],
  },
});
