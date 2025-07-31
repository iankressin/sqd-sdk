import {globSync} from 'glob'
import {defineConfig} from 'tsup'
import {copyFileSync} from 'node:fs'

const entries = globSync('src/**/*.ts', {ignore: ['src/**/*.test.ts']})

export default defineConfig({
    entry: entries,
    outDir: 'lib',
    format: ['cjs', 'esm'],
    bundle: false,
    splitting: false,
    sourcemap: true,
    outExtension({format}) {
        return {
            js: format === 'cjs' ? '.cjs' : '.js',
        }
    },
    async onSuccess() {
        copyFileSync('package.json', 'lib/package.json')
    },
})
