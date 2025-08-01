import {defineConfig} from 'tsup'
import {getBaseTsupConfig} from '../common/tsup-config'

export default defineConfig(getBaseTsupConfig(__dirname))
