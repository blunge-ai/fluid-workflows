import { defineConfig } from 'vite'
import path from 'path'

export default defineConfig({
  resolve: {
    alias: {
      '~': path.resolve(__dirname, './src'),
    },
  },
  test: {
    includeSource: ['src/**/*.{js,ts}'],
    exclude: [
      '**/node_modules/**',
      '**/dist/**',
      '**/*.cloudflare.test.ts',  // Cloudflare-specific tests run with vitest.cloudflare.config.ts
    ],
  },
});
