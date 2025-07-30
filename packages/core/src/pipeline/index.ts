import {withAbort} from '../internal/misc'
import {isForkException} from './errors'
import type {
    Data,
    DataBatch,
    DataFork,
    DataReader,
    DataRef,
    DataCursor,
    DataWriter,
    UnfinalizedDataWriter,
    DataSource as DataSource_,
    UnfinalizedDataSource,
    FinalizedDataSource,
    DataTarget as DataTarget_,
    UnfinalizedDataTarget,
    FinalizedDataTarget,
    DataReaderOptions,
    DataWriterOptions,
} from './types'

export * from './types'

async function pipe<T extends Data>(source: DataSource_<T>, target: DataTarget_<T>): Promise<void> {
    if (source.unfinalized && !target.unfinalized) {
        throw new TypeError('Cannot pipe from unfinalized DataSource to finalized DataTarget')
    }

    try {
        await target.write({cursor: source.cursor}, (opts) => source.read(opts))
    } finally {
        await source.close?.().catch(() => {})
        await target.close?.().catch(() => {})
    }
}

export interface FinalizedDataSourceConfig<T extends Data> {
    reader: (opts: DataReaderOptions<T>) => PromiseLike<DataReader<T>>
    cursor: DataCursor<T>
    unfinalized?: false
}

export interface UnfinalizedDataSourceConfig<T extends Data> {
    reader: (opts: DataReaderOptions<T>) => PromiseLike<DataReader<T>>
    cursor: DataCursor<T>
    unfinalized: true
}

class DataSource<T extends Data> {
    readonly unfinalized: boolean
    readonly cursor: DataCursor<T>

    private _state: 'opened' | 'locked' | 'closed' = 'opened'
    private _abortController: AbortController | undefined

    constructor(config: FinalizedDataSourceConfig<T> | UnfinalizedDataSourceConfig<T>) {
        const {reader, cursor, unfinalized} = config

        this.unfinalized = !!unfinalized
        this.cursor = cursor

        Object.assign(this, {
            _reader: reader,
        })
    }

    private _reader(opts: DataReaderOptions<T>): Promise<DataReader<T>> {
        throw new Error('Not implemented')
    }

    async *read(opts: DataReaderOptions<T>): AsyncIterable<DataBatch<T>> {
        if (this._state === 'closed') {
            throw new Error('DataSource is closed')
        }

        if (this._state === 'locked') {
            throw new Error('DataSource is already locked')
        }
        this._state = 'locked'

        this._abortController = new AbortController()
        const reader = await this._reader(opts)
        try {
            while (true) {
                const batch = await withAbort(() => reader.read(), this._abortController.signal)
                if (!batch) break
                yield batch
            }
        } catch (err) {
            if (!isForkException<T>(err)) throw err
            if (!this.unfinalized) {
                throw new TypeError('Got fork exception in finalized DataSource')
            }
            throw err
        } finally {
            await reader.close?.().catch(() => {})
            this._abortController = undefined
            if (this._state === 'locked') {
                this._state = 'opened'
            }
        }
    }

    pipeThrough<U extends DataSource_<any>>(duplex: {target: DataTarget_<T>; source: U}): U {
        pipe(this, duplex.target).catch((err) => {
            throw err
        })
        return duplex.source
    }

    pipeTo(target: DataTarget_<T>): Promise<void> {
        return pipe(this, target)
    }

    async close(reason?: any): Promise<void> {
        if (this._state === 'closed') return
        this._state = 'closed'
        this._abortController?.abort(reason)
    }
}

export function source<T extends Data>(config: UnfinalizedDataSourceConfig<T>): UnfinalizedDataSource<T>
export function source<T extends Data>(config: FinalizedDataSourceConfig<T>): FinalizedDataSource<T>
export function source<T extends Data>(
    config: FinalizedDataSourceConfig<T> | UnfinalizedDataSourceConfig<T>
): DataSource_<T>
export function source<T extends Data>(
    config: FinalizedDataSourceConfig<T> | UnfinalizedDataSourceConfig<T>
): DataSource_<T> {
    return new DataSource(config)
}

export interface UnfinalizedDataTargetConfig<T extends Data> {
    writer: () => PromiseLike<UnfinalizedDataWriter<T>>
    unfinalized: true
}

export interface FinalizedDataTargetConfig<T extends Data> {
    writer: () => PromiseLike<DataWriter<T>>
    unfinalized?: false
}

class DataTarget<T extends Data> {
    readonly unfinalized: boolean

    private _state: 'opened' | 'locked' | 'closed' = 'opened'
    private _abortController: AbortController | undefined

    constructor(config: FinalizedDataTargetConfig<T> | UnfinalizedDataTargetConfig<T>) {
        this.unfinalized = !!config.unfinalized

        Object.assign(this, {
            _writer: config.writer,
        })
    }

    private _writer(opts: DataWriterOptions<T>): Promise<DataWriter<T>> {
        throw new Error('Not implemented')
    }

    async write(
        opts: DataWriterOptions<T>,
        read: (opts: DataReaderOptions<T>) => AsyncIterable<DataBatch<T>>
    ): Promise<void> {
        if (this._state === 'closed') {
            throw new Error('DataTarget is closed')
        }

        if (this._state === 'locked') {
            throw new Error('DataTarget is already locked')
        }
        this._state = 'locked'

        this._abortController = new AbortController()
        const writer = await this._writer(opts)
        try {
            await withAbort(async () => {
                let offset = writer.offset
                while (true) {
                    try {
                        for await (const batch of read({offset})) {
                            await writer.write(batch)
                        }
                        break
                    } catch (err) {
                        if (!isForkException<T>(err)) throw err
                        if (!this.unfinalized) {
                            throw new TypeError('Got fork exception in finalized DataTarget')
                        }
                        if (!writer.fork) {
                            throw new TypeError('Missing fork method in unfinalized DataWriter')
                        }
                        offset = await writer.fork(err.fork)
                    }
                }
            }, this._abortController.signal)
        } finally {
            await writer.close?.()
            this._abortController = undefined
            if (this._state === 'locked') {
                this._state = 'opened'
            }
        }
    }

    async close(reason?: any): Promise<void> {
        if (this._state === 'closed') return
        this._state = 'closed'
        this._abortController?.abort(reason)
    }
}

export function target<T extends Data>(config: UnfinalizedDataTargetConfig<T>): UnfinalizedDataTarget<T>
export function target<T extends Data>(config: FinalizedDataTargetConfig<T>): FinalizedDataTarget<T>
export function target<T extends Data>(
    config: FinalizedDataTargetConfig<T> | UnfinalizedDataTargetConfig<T>
): DataTarget_<T>
export function target<T extends Data>(
    config: FinalizedDataTargetConfig<T> | UnfinalizedDataTargetConfig<T>
): DataTarget_<T> {
    return new DataTarget(config)
}

export interface UnfinalizedTransformerConfig<T extends Data, U extends Data> {
    transform: (batch: DataBatch<T>) => Promise<DataBatch<U>>
    fork: (fork: DataFork<T>, cursor: DataCursor<T>) => Promise<DataFork<U>>
    flush?: () => Promise<DataBatch<U>>
    cursor: DataCursor<U>
    unfinalized: true
}

export interface FinalizedTransformerConfig<T extends Data, U extends Data> {
    transform: (batch: DataBatch<T>) => Promise<DataBatch<U>>
    fork?: (fork: DataFork<T>) => Promise<DataFork<U>>
    flush?: () => Promise<DataBatch<U>>
    cursor: DataCursor<U>
    unfinalized: false
}

// export function transformer<T extends Data, U extends Data>(
//     config: UnfinalizedTransformerConfig<T, U>
// ): {target: UnfinalizedDataTarget<U>; source: UnfinalizedDataSource<T>}
// export function transformer<T extends Data, U extends Data>(
//     config: FinalizedTransformerConfig<T, U>
// ): {target: FinalizedDataTarget<U>; source: FinalizedDataSource<T>}
// export function transformer<T extends Data, U extends Data>(
//     config: UnfinalizedTransformerConfig<T, U> | FinalizedTransformerConfig<T, U>
// ): DataDuplex<T, U>
// export function transformer<T extends Data, U extends Data>(
//     config: UnfinalizedTransformerConfig<T, U> | FinalizedTransformerConfig<T, U>
// ): DataDuplex<T, U> {
//     let dataFork: DataFork<U> | undefined = undefined

//     let headFuture: Future<DataRef<U>> | undefined = undefined
//     let dataFuture: Future<DataBatch<U>> = createFuture<DataBatch<U>>()
//     let readyFuture: Future<void> = createFuture<void>()

//     const targetInstance = target<T>({
//         unfinalized: config.unfinalized,
//         writer: async () => {
//             const offset = await headFuture?.promise()
//             return {
//                 offset,
//                 async write(batch: DataBatch<T>): Promise<void> {
//                     await readyFuture.promise()
//                     const result = await config.transform(batch)
//                     dataFuture.resolve(result)
//                     dataFuture = createFuture<DataBatch<U>>()
//                 },
//                 async fork(fork: DataFork<T>): Promise<DataRef<T> | undefined> {
//                     dataFork = await config.fork?.(fork)

//                     headFuture?.resolve(undefined)
//                     headFuture = createFuture<DataRef<U>>()

//                     return headFuture.promise()
//                 },
//                 async close(): Promise<void> {
//                     await sourceInstance.close()
//                 },
//             }
//         },
//     })

//     const sourceInstance = source<U>({
//         unfinalized: config.unfinalized,
//         reader: async (offset) => {
//             headFuture?.resolve(offset)
//             return {
//                 async read(): Promise<DataBatch<U> | null> {
//                     if (dataFork) {
//                         throw new ForkException(dataFork)
//                     }

//                     readyFuture.resolve()
//                     readyFuture = createFuture<void>()

//                     return await dataFuture.promise()
//                 },
//                 async close(): Promise<void> {
//                     await targetInstance.close()
//                 },
//             }
//         },
//     })

//     return {
//         target: targetInstance,
//         source: sourceInstance,
//     }
// }
