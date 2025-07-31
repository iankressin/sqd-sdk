import {defineConfig} from 'tsup'
import {globSync} from 'glob'
import fs from 'node:fs'

const entries = globSync('src/**/*.ts', {ignore: ['src/**/*.test.ts']})

export default defineConfig({
    entry: entries,
    outDir: 'lib',
    format: ['cjs', 'esm'],
    bundle: true,
    clean: true,
    dts: true,
    splitting: false,
    sourcemap: true,
    outExtension({format}) {
        return {
            js: format === 'cjs' ? '.cjs' : '.js',
        }
    },
    async onSuccess() {
        console.log('Generating package.json exports...')

        const pkg = JSON.parse(fs.readFileSync('package.json', 'utf8'))

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

        fs.writeFileSync('./lib/package.json', JSON.stringify(pkg, null, 2))
    },
})
