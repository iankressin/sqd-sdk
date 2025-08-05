import {assert} from '../internal/misc'
import type {Data, DataFork} from './data'

export class ForkException<D extends Data> extends Error {
    readonly isSqdForkException = true

    constructor(readonly fork: DataFork<D>) {
        assert(fork.heads.length > 0)
        const lastRef = fork.heads[fork.heads.length - 1]
        super(`Fork exception at ${lastRef}`)
    }

    override get name(): string {
        return 'ForkException'
    }
}

export const isForkException = <D extends Data>(err: unknown): err is ForkException<D> =>
    err instanceof Error && !!(err as Partial<ForkException<D>>).isSqdForkException
