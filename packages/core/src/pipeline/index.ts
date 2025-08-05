import {createFuture, type Future, SyncQueue} from '../internal/async'
import {withAbort} from '../internal/misc'
import {isForkException} from './errors'
import type {Data, DataBatch, DataFork, DataRef} from './data'

export * from './errors'
export * from './data'

export interface DataReaderOptions<T extends Data> {
    offset?: T['ref'] | undefined
}

export interface DataReader<T extends Data> {
    read(): Promise<DataBatch<T> | undefined>
    close?(): Promise<unknown>
}

export type DataSource<T extends Data> = FinalizedDataSource<T> | UnfinalizedDataSource<T>

export interface FinalizedDataSource<T extends Data> extends AsyncIterable<T['value']> {
    readonly unfinalized: false
    read(opts: DataReaderOptions<T>): AsyncIterable<DataBatch<T>>
    pipeThrough<U extends DataSource<any>>(duplex: PipableThrough<{target: DataTarget<T>; source: U}>): U
    pipeTo(target: DataTarget<T>): Promise<void>
    close(): Promise<void>
}

export interface UnfinalizedDataSource<T extends Data> extends AsyncIterable<T['value']> {
    readonly unfinalized: true
    read(opts: DataReaderOptions<T>): AsyncIterable<DataBatch<T>>
    pipeThrough<U extends DataSource<any>>(duplex: PipableThrough<{target: UnfinalizedDataTarget<T>; source: U}>): U
    pipeTo(target: UnfinalizedDataTarget<T>): Promise<void>
    close(): Promise<void>
}

export interface DataWriterOptions<T extends Data> {
    read: (opts: DataReaderOptions<T>) => AsyncIterable<DataBatch<T>>
}

// biome-ignore lint/suspicious/noEmptyInterface: <explanation>
export interface DataWriterWriteOptions<T extends Data> {}

export interface FinalizedDataWriter<T extends Data> {
    offset: T['ref'] | undefined
    write(batch: DataBatch<T>, offset: T['ref'] | undefined): Promise<T['ref']>
    fork?(fork: DataFork<T>, offset: T['ref'] | undefined): Promise<T['ref'] | undefined>
    close?(): Promise<unknown>
}

export interface UnfinalizedDataWriter<T extends Data> extends FinalizedDataWriter<T> {
    fork(fork: DataFork<T>, offset: T['ref'] | undefined): Promise<T['ref'] | undefined>
}

export type DataWriter<T extends Data> = FinalizedDataWriter<T> | UnfinalizedDataWriter<T>

export type DataTarget<T extends Data> = UnfinalizedDataTarget<T> | FinalizedDataTarget<T>

export interface UnfinalizedDataTarget<T extends Data> {
    unfinalized: true
    write(opts: DataWriterOptions<T>): Promise<void>
    close(): Promise<void>
}

export interface FinalizedDataTarget<T extends Data> {
    unfinalized: false
    write(opts: DataWriterOptions<T>): Promise<void>
    close(): Promise<void>
}

export interface DataDuplex<T extends Data, U extends Data> {
    target: DataTarget<T>
    source: DataSource<U>
}

export type DataDuplexFactory<T extends DataDuplex<any, any>> = (
    source: DataSource<T['target'] extends DataTarget<infer U> ? U : never>,
) => T

export type PipableThrough<T extends DataDuplex<any, any>> = T | DataDuplexFactory<T>

async function pipe<T extends Data>(source: DataSource<T>, target: DataTarget<T>): Promise<void> {
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

// NOTE: workaround to allow constructor overloading
export const DataSource: {
    new <T extends Data>(config: UnfinalizedDataSourceConfig<T>): UnfinalizedDataSource<T>
    new <T extends Data>(config: FinalizedDataSourceConfig<T>): FinalizedDataSource<T>
    new <T extends Data>(config: FinalizedDataSourceConfig<T> | UnfinalizedDataSourceConfig<T>): DataSource<T>
} = class<T extends Data> {
    readonly unfinalized: any // FIXME: how to type this?

    private _state: 'opened' | 'locked' | 'closed' = 'opened'
    private _abortController: AbortController | undefined
    private _reader: (opts: DataReaderOptions<T>) => PromiseLike<DataReader<T>>

    constructor(config: FinalizedDataSourceConfig<T> | UnfinalizedDataSourceConfig<T>) {
        const {reader, unfinalized} = config

        this.unfinalized = unfinalized !== false
        this._reader = reader
    }

    async *read(opts: DataReaderOptions<T> = {}): AsyncIterable<DataBatch<T>> {
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

    pipeThrough<U extends DataSource<any>>(duplex: PipableThrough<{target: DataTarget<T>; source: U}>): U {
        if (typeof duplex === 'function') {
            duplex = duplex(this)
        }

        pipe(this, duplex.target).catch((err) => {
            throw err
        })
        return duplex.source
    }

    pipeTo(target: DataTarget<T>): Promise<void> {
        return pipe(this, target)
    }

    async close(reason?: any): Promise<void> {
        if (this._state === 'closed') return
        this._state = 'closed'
        this._abortController?.abort(reason)
    }

    async *[Symbol.asyncIterator](): AsyncIterableIterator<T['value']> {
        for await (const batch of this.read()) {
            for (const data of batch.data) {
                yield data.value
            }
        }
    }
}

// FIXME: which approach is better: function or class?
export function source<T extends Data>(config: UnfinalizedDataSourceConfig<T>): UnfinalizedDataSource<T>
export function source<T extends Data>(config: FinalizedDataSourceConfig<T>): FinalizedDataSource<T>
export function source<T extends Data>(
    config: FinalizedDataSourceConfig<T> | UnfinalizedDataSourceConfig<T>,
): DataSource<T>
export function source<T extends Data>(
    config: FinalizedDataSourceConfig<T> | UnfinalizedDataSourceConfig<T>,
): DataSource<T> {
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

function validateBatch(offset: DataRef<any> | undefined, batch: DataBatch<any>) {
    if (offset && batch.offset.compare(offset) < 0) {
        throw new Error('New offset is below the previous offset')
    }

    for (const item of batch.data) {
        if (offset && item.ref.compare(offset) <= 0) {
            throw new Error('Item is below the previous item')
        }
        offset = item.ref
    }

    if (offset && batch.head.compare(offset) < 0) {
        throw new Error('Head is below the data')
    }

    if (batch.finalizedHead && batch.head.compare(batch.finalizedHead) < 0) {
        throw new Error('Head is below the finalized head')
    }

    if (batch.head.compare(batch.offset) < 0) {
        throw new Error('Head is below the offset')
    }
}

// NOTE: workaround to allow constructor overloading
export const DataTarget: {
    new <T extends Data>(config: UnfinalizedDataTargetConfig<T>): UnfinalizedDataTarget<T>
    new <T extends Data>(config: FinalizedDataTargetConfig<T>): FinalizedDataTarget<T>
    new <T extends Data>(config: FinalizedDataTargetConfig<T> | UnfinalizedDataTargetConfig<T>): DataTarget<T>
} = class<T extends Data> {
    readonly unfinalized: any // FIXME: how to type this?

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
                            validateBatch(offset, batch)

                            offset = await writer.write(batch, offset)
                            // NOTE: If the offset is not the same as the batch offset,
                            // it means that the batch was not fully consumed or we want to skip
                            // so we break the current stream and start from the new offset
                            // FIXME: Do we want this behavior?
                            if (!offset || offset.compare(batch.offset) !== 0) {
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

// FIXME: which approach is better: function or class?
export function target<T extends Data>(config: UnfinalizedDataTargetConfig<T>): UnfinalizedDataTarget<T>
export function target<T extends Data>(config: FinalizedDataTargetConfig<T>): FinalizedDataTarget<T>
export function target<T extends Data>(
    config: FinalizedDataTargetConfig<T> | UnfinalizedDataTargetConfig<T>,
): DataTarget<T>
export function target<T extends Data>(
    config: FinalizedDataTargetConfig<T> | UnfinalizedDataTargetConfig<T>,
): DataTarget<T> {
    return new DataTarget(config)
}

export interface DataTransformer<T extends Data, U extends Data> {
    offset: T['ref'] | undefined
    transform: (batch: DataBatch<T>) => Promise<DataBatch<U>>
    fork?: (fork: DataFork<T>) => Promise<DataFork<U>>
}

export interface TransformerOptions<T extends Data> {
    offset: T['ref'] | undefined
}

export interface TransformerConfig<T extends Data, U extends Data> {
    transformer: (opts: TransformerOptions<U>) => PromiseLike<DataTransformer<T, U>>
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
        let offsetFuture: Future<U['ref'] | undefined> = createFuture()

        const target = new DataTarget<T>({
            unfinalized: parent.unfinalized,
            writer: async (opts: DataWriterOptions<T>) => {
                const transformer = await config.transformer({
                    offset: await offsetFuture.promise(),
                })
                if (parent.unfinalized && !transformer.fork) {
                    throw new TypeError('Missing fork method in unfinalized DataTransformer')
                }

                return {
                    offset: transformer.offset,
                    async write(batch: DataBatch<T>) {
                        const data = await transformer.transform(batch)
                        await queue.put(data)
                        return batch.offset
                    },
                    async fork(fork: DataFork<T>): Promise<DataFork<U> | undefined> {
                        return transformer.fork?.(fork)
                    },
                    async close(): Promise<void> {
                        await source.close()
                    },
                }
            },
        })

        const source = new DataSource<U>({
            unfinalized: parent.unfinalized,
            reader: async (opts) => {
                offsetFuture.resolve(opts.offset)

                return {
                    async read(): Promise<DataBatch<U> | undefined> {
                        return await queue.take()
                    },
                    async close(): Promise<void> {
                        await target.close()
                    },
                }
            },
        })

        return {
            target,
            source,
        }
    }
}

export function finalizer<T extends Data>(): {target: UnfinalizedDataTarget<T>; source: FinalizedDataSource<T>} {
    const buffer: T[] = []
    const queue = new SyncQueue<DataBatch<T>>()
    let offsetFuture: Future<T['ref'] | undefined> = createFuture()

    const target = new DataTarget<T>({
        unfinalized: true,
        writer: async (opts: DataWriterOptions<T>) => {
            const offset = await offsetFuture.promise()
            return {
                offset,
                async write(batch: DataBatch<T>) {
                    buffer.push(...batch.data)

                    if (batch.finalizedHead) {
                        let unfinalizedIndex = 0
                        for (; unfinalizedIndex < buffer.length; unfinalizedIndex++) {
                            const ref = buffer[unfinalizedIndex].ref
                            if (batch.head.compare(ref) !== 'gt') break
                        }

                        const data = buffer.slice(0, unfinalizedIndex)
                        const offset = data[data.length - 1].ref

                        await queue.put({
                            data,
                            offset,
                            finalizedHead: batch.finalizedHead,
                            head: batch.finalizedHead,
                        })
                    }

                    return batch.offset
                },
                async fork(fork: DataFork<T>): Promise<DataRef<T> | undefined> {
                    return undefined
                },
                async close(): Promise<void> {
                    await target.close()
                },
            }
        },
    })

    const source = new DataSource<T>({
        unfinalized: false,
        reader: async (opts) => {
            offsetFuture.resolve(opts.offset)

            return {
                async read(): Promise<DataBatch<T> | undefined> {
                    return await queue.take()
                },
                async close(): Promise<void> {
                    await target.close()
                },
            }
        },
    })

    return {
        target,
        source,
    }
}
