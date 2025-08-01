import {defineConfig} from 'tsup'
import {globSync} from 'glob'
import {updatePackageExportsPath} from '../../scripts/package-exports-path'

const entries = globSync('src/**/*.ts', {ignore: ['src/**/*.test.ts']})

export default defineConfig({
    entry: entries,
    outDir: 'lib',
    format: 'cjs',
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
        __VERSION__: 'dev'
    },
})
