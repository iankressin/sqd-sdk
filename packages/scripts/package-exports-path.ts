import {globSync} from 'glob'
import {readFileSync, writeFileSync} from 'node:fs'

export function updatePackageExportsPath(basePath: string) {
    console.log('Generating package.json exports...')

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
                require: {
                    types: string
                    default: string
                }
                default: string
                types: string
            }
        >
    >((acc, rawEntry) => {
        const entry = rawEntry.match(/src\/(.*)\.ts/)![1]!
        const exportsEntry = entry === 'index' ? '.' : `./${entry.replace(/\/index$/, '')}`
        const importEntry = `./${entry}.js`
        const requireEntry = `./${entry}.cjs`
        acc[exportsEntry] = {
            import: {
                types: `./${entry}.d.ts`,
                default: importEntry,
            },
            require: {
                types: `./${entry}.d.cts`,
                default: requireEntry,
            },
            types: `./${entry}.d.ts`,
            default: importEntry,
        }
        return acc
    }, {})

    writeFileSync(`${basePath}/lib/package.json`, JSON.stringify(pkg, null, 2))
}
