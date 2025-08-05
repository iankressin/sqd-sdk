import type {Options} from 'tsup'
import {globSync} from 'glob'
import {updatePackageExportsPath} from './package-exports-path'
import {readFileSync} from 'node:fs'
import {BUILD_FOLDER} from './constants'

export function getBaseTsupConfig(basePath: string): Options {
    const entries = globSync(`${basePath}/src/**/*.ts`, {ignore: [`${basePath}/src/**/*.test.ts`]})

    return {
        entry: entries,
        outDir: BUILD_FOLDER,
        format: ['cjs', 'esm'],
        bundle: false,
        dts: true,
        clean: true,
        splitting: false,
        sourcemap: true,
        outExtension({format}) {
            return {
                js: format === 'cjs' ? '.cjs' : '.js',
            }
        },
        async onSuccess() {
            updatePackageExportsPath(basePath)
        },
        define: {
            __VERSION__: JSON.stringify(JSON.parse(readFileSync('package.json', 'utf8')).version),
        },
    }
}

export function getDevBaseTsupConfig(basePath: string): Options {
    const entries = globSync(`${basePath}/src/**/*.ts`, {ignore: [`${basePath}/src/**/*.test.ts`]})

    return {
        entry: entries,
        outDir: BUILD_FOLDER,
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
            updatePackageExportsPath(basePath)
        },
        define: {
            __VERSION__: 'dev',
        },
    }
}
