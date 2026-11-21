import { defineConfig } from 'vite';
import { tanstackStart } from '@tanstack/solid-start/plugin/vite';
import solidPlugin from 'vite-plugin-solid';
import tailwindcss from '@tailwindcss/vite';
import { nitro } from 'nitro/vite';

export default defineConfig({
  resolve: { tsconfigPaths: true },
  plugins: [tanstackStart(), nitro(), solidPlugin({ ssr: true }), tailwindcss()],
});
