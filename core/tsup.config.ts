import {defineConfig} from 'tsup'
import {esbuildPluginFilePathExtensions} from 'esbuild-plugin-file-path-extensions'

export default defineConfig({
    entry: ['src/**/*.ts'],
    outDir: 'lib',
    format: ['cjs', 'esm'],
    bundle: true,
    clean: true,
    dts: true,
    splitting: false,
    sourcemap: true,
    esbuildPlugins: [esbuildPluginFilePathExtensions({cjsExtension: 'cjs', esmExtension: 'js'})],
})
