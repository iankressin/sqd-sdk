import {globSync} from 'glob'
import {defineConfig} from 'tsup'
import {readFileSync} from 'node:fs'
import {updatePackageExportsPath} from '../scripts/package-exports-path'

const entries = globSync('src/**/*.ts', {ignore: ['src/**/*.test.ts']})

export default defineConfig({
    entry: entries,
    outDir: 'lib',
    format: ['cjs', 'esm'],
    bundle: false,
    clean: true,
    splitting: false,
    sourcemap: true,
    outExtension({format}) {
        return {
            js: format === 'cjs' ? '.cjs' : '.js',
        }
    },
    async onSuccess() {
        updatePackageExportsPath(__dirname)
    },
    define: {
        __VERSION__: JSON.stringify(JSON.parse(readFileSync('package.json', 'utf8')).version),
    },
})
