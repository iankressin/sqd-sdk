import {defineConfig} from 'tsup'
import {getDevBaseTsupConfig} from '../../common/tsup-config'

export default defineConfig(getDevBaseTsupConfig(__dirname))
