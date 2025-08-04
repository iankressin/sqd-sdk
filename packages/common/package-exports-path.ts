import {globSync} from 'glob'
import {readFileSync, writeFileSync} from 'node:fs'
import {BUILD_FOLDER} from './constants'

export function updatePackageExportsPath(basePath: string) {
    console.log('Generating package.json exports for ', basePath)

    const entries = globSync(`${basePath}/src/**/*.ts`, {
        ignore: [`${basePath}/src/**/*.test.ts`],
    })
    const pkg = JSON.parse(readFileSync('package.json', 'utf8'))

    pkg.exports = entries.reduce<
        Record<
            string,
            {
                import: {
                    types?: string
                    default: string
                }
                types: string
                default: string
            }
        >
    >((acc, rawEntry) => {
        const entry = rawEntry.match(/src\/(.*)\.ts/)![1]!
        const exportsEntry = entry === 'index' ? '.' : `./${entry.replace(/\/index$/, '')}`
        const importEntry = `./${BUILD_FOLDER}/${entry}.js`
        const requireEntry = `./${BUILD_FOLDER}/${entry}.cjs`
        acc[exportsEntry] = {
            import: {
                types: `./${BUILD_FOLDER}/${entry}.d.mts`,
                default: importEntry,
            },
            types: `./${BUILD_FOLDER}/${entry}.d.ts`,
            default: requireEntry,
        }
        return acc
    }, {})

    writeFileSync(`${basePath}/package.json`, JSON.stringify(pkg, null, 2))

    console.log('Exports generated successfully')
}
