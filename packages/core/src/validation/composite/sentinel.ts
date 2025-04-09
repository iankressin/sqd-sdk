import type {ValidationFailure} from '../error'
import type {Validator} from '../interface'

const SUPPRESSED_LABELS: {
    [label: string]: boolean
} = initSuppressedLabels()

function warn(label: string): void {}

export class Sentinel<T, S> implements Validator<T, S | null | undefined> {
    constructor(
        public readonly label: string,
        public readonly value: T,
        public readonly item: Validator<T, S>,
    ) {}

    cast(value: unknown): ValidationFailure | T {
        if (value == null) {
            warn(this.label)
            return this.value
        }
        return this.item.cast(value)
    }

    validate(value: unknown): ValidationFailure | undefined {
        if (value == null) return
        return this.item.validate(value)
    }

    phantom(): S | null | undefined {
        return undefined
    }
}

function initSuppressedLabels(): Record<string, boolean> {
    let rec: Record<string, boolean> = {}
    // if (typeof process.env.SQD_ALLOW_SENTINEL === 'string') {
    //     let labels = process.env.SQD_ALLOW_SENTINEL.split(',').map((l) => l.trim())
    //     for (let l of labels) {
    //         rec[l] = true
    //     }
    // }
    return rec
}
