// import fs from 'node:fs/promises'
import {globSync} from 'glob'
import {defineConfig} from 'tsup'
import fs from 'node:fs'

export default defineConfig({
    entry: globSync('src/**/*.ts'),
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
        console.log('Generating package.json exports...')

        const entries = globSync('src/*/')
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
            const entry = rawEntry.replace(/^src\//, '')
            const exportsEntry = `./${entry}`
            const importEntry = `./${entry}/index.js`
            const requireEntry = `./${entry}/index.cjs`
            acc[exportsEntry] = {
                import: {
                    types: `./${entry}/index.d.ts`,
                    default: importEntry,
                },
                require: {
                    types: `./${entry}/index.d.cts`,
                    default: requireEntry,
                },
                types: `./${entry}/index.d.ts`,
                default: importEntry,
            }
            return acc
        }, {})

        fs.writeFileSync('./lib/package.json', JSON.stringify(pkg, null, 2))
    },
})
