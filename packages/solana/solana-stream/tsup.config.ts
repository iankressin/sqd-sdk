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
    outExtension: ({format}) => (format === 'cjs' ? {js: '.js', dts: '.d.ts'} : {js: '.mjs', dts: '.d.mts'}),
    esbuildPlugins: [esbuildPluginFilePathExtensions({cjsExtension: 'js', esmExtension: 'mjs'})],
})
