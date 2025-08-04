import {createFuture, type Future, SyncQueue} from '../internal/async'
import {withAbort} from '../internal/misc'
import {isForkException} from './errors'
import type {
    Data,
    DataBatch,
    DataFork,
    DataReader,
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
    DataDuplex,
    DataRef,
    PipableThrough,
    DataDuplexFactory,
    DataItem,
} from './types'

export * from './types'
export * from './errors'

async function pipe<T extends Data>(source: DataSource_<T>, target: DataTarget_<T>): Promise<void> {
    if (source.unfinalized && !target.unfinalized) {
        throw new TypeError('Cannot pipe from unfinalized DataSource to finalized DataTarget')
    }

    try {
        await target.write(source)
    } finally {
        await source.close?.().catch(() => {})
        await target.close?.().catch(() => {})
    }
}

export interface FinalizedDataSourceConfig<T extends Data> {
    reader: (opts: DataReaderOptions<T>) => PromiseLike<DataReader<T>>
    unfinalized: false
}

export interface UnfinalizedDataSourceConfig<T extends Data> {
    reader: (opts: DataReaderOptions<T>) => PromiseLike<DataReader<T>>
    unfinalized?: true
}

class DataSource<T extends Data> {
    readonly unfinalized: boolean

    private _state: 'opened' | 'locked' | 'closed' = 'opened'
    private _abortController: AbortController | undefined
    private _reader: (opts: DataReaderOptions<T>) => PromiseLike<DataReader<T>>

    constructor(config: FinalizedDataSourceConfig<T> | UnfinalizedDataSourceConfig<T>) {
        const {reader, unfinalized} = config

        this.unfinalized = unfinalized !== false
        this._reader = reader
    }

    async *read(opts: DataReaderOptions<T>): AsyncIterable<DataBatch<T>> {
        if (this._state === 'closed') {
            throw new Error('DataSource is already closed')
        }

        if (this._state === 'locked') {
            throw new Error('DataSource is locked')
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

    pipeThrough<U extends DataSource_<any>>(duplex: PipableThrough<{target: DataTarget_<T>; source: U}>): U {
        if (typeof duplex === 'function') {
            duplex = duplex(this)
        }

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
    config: FinalizedDataSourceConfig<T> | UnfinalizedDataSourceConfig<T>,
): DataSource_<T>
export function source<T extends Data>(
    config: FinalizedDataSourceConfig<T> | UnfinalizedDataSourceConfig<T>,
): DataSource_<T> {
    return new DataSource(config)
}

export interface UnfinalizedDataTargetConfig<T extends Data> {
    writer: (opts: DataWriterOptions<T>) => PromiseLike<UnfinalizedDataWriter<T>>
    unfinalized?: true
}

export interface FinalizedDataTargetConfig<T extends Data> {
    writer: (opts: DataWriterOptions<T>) => PromiseLike<DataWriter<T>>
    unfinalized: false
}

class DataTarget<T extends Data> {
    readonly unfinalized: boolean

    private _state: 'opened' | 'locked' | 'closed' = 'opened'
    private _abortController: AbortController | undefined
    private _writer: (opts: DataWriterOptions<T>) => PromiseLike<DataWriter<T>>

    constructor(config: FinalizedDataTargetConfig<T> | UnfinalizedDataTargetConfig<T>) {
        this.unfinalized = config.unfinalized !== false

        this._writer = config.writer
    }

    async write(opts: DataWriterOptions<T>): Promise<void> {
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
                let isRetry: boolean
                while (true) {
                    // FIXME: really bad
                    isRetry = false
                    try {
                        for await (const batch of opts.read({offset})) {
                            offset = await writer.write(batch, offset)
                            // NOTE: If the offset is not the same as the batch offset,
                            // it means that the batch was not fully consumed or we want to skip
                            // so we break the current stream and start from the new offset
                            // FIXME: Do we want this behavior?
                            if (batch.cursor.compare(offset, batch.offset) !== 'eq') {
                                isRetry = true
                                break
                            }
                        }
                        if (!isRetry) break
                    } catch (err) {
                        if (!isForkException<T>(err)) throw err
                        if (!this.unfinalized) {
                            throw new TypeError('Got fork exception in finalized DataTarget')
                        }
                        if (!writer.fork) {
                            throw new TypeError('Missing fork method in unfinalized DataWriter')
                        }
                        offset = await writer.fork(err.fork, offset)
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
    config: FinalizedDataTargetConfig<T> | UnfinalizedDataTargetConfig<T>,
): DataTarget_<T>
export function target<T extends Data>(
    config: FinalizedDataTargetConfig<T> | UnfinalizedDataTargetConfig<T>,
): DataTarget_<T> {
    return new DataTarget(config)
}

export interface DataTransformer<T extends Data, U extends Data> {
    transform: (batch: DataBatch<T>) => Promise<DataBatch<U>>
    fork?: (fork: DataFork<T>) => Promise<DataFork<U>>
}

// biome-ignore lint/suspicious/noEmptyInterface: <explanation>
export interface TransformerOptions<T extends Data> {}

export interface TransformerConfig<T extends Data, U extends Data> {
    transformer: (opts: TransformerOptions<T>) => PromiseLike<DataTransformer<T, U>>
}

export function transformer<T extends Data, U extends Data>(
    config: TransformerConfig<T, U>,
): DataDuplexFactory<{target: UnfinalizedDataTarget<T>; source: UnfinalizedDataSource<U>}>
export function transformer<T extends Data, U extends Data>(
    config: TransformerConfig<T, U>,
): DataDuplexFactory<{target: FinalizedDataTarget<T>; source: FinalizedDataSource<U>}>
export function transformer<T extends Data, U extends Data>(
    config: TransformerConfig<T, U>,
): DataDuplexFactory<DataDuplex<T, U>> {
    return (parent) => {
        const queue = new SyncQueue<DataBatch<U>>()
        let targetInit: Future<DataReaderOptions<U>> = createFuture<DataReaderOptions<U>>()
        let sourceInit: Future<DataWriterOptions<T>> = createFuture<DataWriterOptions<T>>()

        const targetInstance = target<T>({
            unfinalized: parent.unfinalized,
            writer: async (opts: DataWriterOptions<T>) => {
                const transformer = await config.transformer({})
                if (parent.unfinalized && !transformer.fork) {
                    throw new TypeError('Missing fork method in unfinalized DataTransformer')
                }

                const {offset} = await targetInit.promise()
                return {
                    offset,
                    async write(batch: DataBatch<T>) {
                        const data = await transformer.transform(batch)
                        await queue.put(data)
                        return batch.offset
                    },
                    async fork(fork: DataFork<T>): Promise<DataRef<T> | undefined> {
                        return transformer.fork?.(fork)
                    },
                    async close(): Promise<void> {
                        await targetInstance.close()
                    },
                }
            },
        })

        const sourceInstance = source<U>({
            unfinalized: parent.unfinalized,
            reader: async (opts) => {
                targetInit.resolve(opts)
                const init = await sourceInit.promise()

                return {
                    async read(): Promise<DataBatch<U> | undefined> {
                        return await queue.take()
                    },
                    async close(): Promise<void> {
                        await targetInstance.close()
                    },
                }
            },
        })

        return {
            target: targetInstance,
            source: sourceInstance,
        }
    }
}

export function finalizer<T extends Data>(): {target: UnfinalizedDataTarget<T>; source: FinalizedDataSource<T>} {
    const buffer: DataItem<T>[] = []
    const queue = new SyncQueue<DataBatch<T>>()
    let targetInit: Future<DataReaderOptions<T>> = createFuture<DataReaderOptions<T>>()
    let sourceInit: Future<DataWriterOptions<T>> = createFuture<DataWriterOptions<T>>()

    const targetInstance = target<T>({
        unfinalized: true,
        writer: async (opts: DataWriterOptions<T>) => {
            const {offset} = await targetInit.promise()
            return {
                offset,
                async write(batch: DataBatch<T>) {
                    buffer.push(...batch.data)
                    return batch.offset
                },
                async fork(fork: DataFork<T>): Promise<DataRef<T> | undefined> {
                    return undefined
                },
                async close(): Promise<void> {
                    await targetInstance.close()
                },
            }
        },
    })

    const sourceInstance = source<T>({
        unfinalized: false,
        reader: async (opts) => {
            targetInit.resolve(opts)
            const init = await sourceInit.promise()

            return {
                async read(): Promise<DataBatch<T> | undefined> {
                    return await queue.take()
                },
                async close(): Promise<void> {
                    await targetInstance.close()
                },
            }
        },
    })

    return {
        target: targetInstance,
        source: sourceInstance,
    }
}
