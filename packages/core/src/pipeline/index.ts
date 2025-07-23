import {createFuture, type Future} from '../internal/async'
import {last} from '../internal/misc'
import {ForkException, isForkException} from './errors'
import type {
    Data,
    DataBatch,
    DataDuplex,
    DataFork,
    DataReader,
    DataRef,
    DataRefer,
    DataSource,
    DataTarget,
    DataWriter,
    FinalizedDataSource,
    FinalizedDataTarget,
    UnfinalizedDataSource,
    UnfinalizedDataTarget,
    UnfinalizedDataWriter,
} from './types'

export * from './types'
export * from './errors'

export interface FinalizedDataSourceConfig<T extends Data> {
    reader: (offset?: DataRef<T>) => PromiseLike<DataReader<T>>
    ref: DataRefer<T>
    finalized: true
}

export interface UnfinalizedDataSourceConfig<T extends Data> {
    reader: (offset?: DataRef<T>) => PromiseLike<DataReader<T>>
    ref: DataRefer<T>
    finalized?: false
}

function finalizedSource<T extends Data>(config: {
    reader: (offset?: DataRef<T>) => PromiseLike<DataReader<T>>
    ref: DataRefer<T>
}): FinalizedDataSource<T> {
    const {reader, ref} = config
    let currentDataReader: DataReader<T> | undefined
    let currentOffset: DataRef<T> | undefined

    return {
        ref,
        finalized: true,
        async read(offset?: DataRef<T>) {
            if (ref.compare(offset, currentOffset) !== 'gt') {
                await currentDataReader?.close?.()
                currentDataReader = undefined
                currentOffset = offset
            }

            if (!currentDataReader) {
                currentDataReader = await reader(currentOffset)
            }

            const batch = await currentDataReader.read()
            if (batch && batch.data.length > 0) {
                currentOffset = ref.get(last(batch.data))
            }

            return batch
        },
        async close() {
            await currentDataReader?.close?.()
            currentDataReader = undefined
        },
        pipeThrough<U extends Data, F extends boolean>(duplex: {
            target: DataTarget<T>
            source: DataSource<U, F>
        }): DataSource<U, F> {
            pipe(this, duplex.target).catch((e) => {
                throw e
            })
            return duplex.source
        },
        pipeTo(target: DataTarget<T>): Promise<void> {
            return pipe(this, target)
        },
    }
}

function unfinalizedSource<T extends Data>(config: {
    reader: (offset?: DataRef<T>) => PromiseLike<DataReader<T>>
    ref: DataRefer<T>
}): UnfinalizedDataSource<T> {
    const {reader, ref} = config
    let currentDataReader: DataReader<T> | undefined
    let currentOffset: DataRef<T> | undefined

    return {
        ref,
        finalized: false,
        async read(offset?: DataRef<T>) {
            if (ref.compare(offset, currentOffset) !== 'gt') {
                await currentDataReader?.close?.()
                currentDataReader = undefined
                currentOffset = offset
            }

            if (!currentDataReader) {
                currentDataReader = await reader(currentOffset)
            }

            const batch = await currentDataReader.read()
            if (batch && batch.data.length > 0) {
                currentOffset = ref.get(last(batch.data))
            }

            return batch
        },
        async close() {
            await currentDataReader?.close?.()
            currentDataReader = undefined
        },
        pipeThrough<U extends Data, F extends boolean>(duplex: {
            target: UnfinalizedDataTarget<T>
            source: DataSource<U, F>
        }): DataSource<U, F> {
            pipe(this, duplex.target).catch((e) => {
                throw e
            })
            return duplex.source
        },
        pipeTo(target: UnfinalizedDataTarget<T>): Promise<void> {
            return pipe(this, target)
        },
    }
}

export function source<T extends Data>(config: {
    reader: (offset?: DataRef<T>) => PromiseLike<DataReader<T>>
    ref: DataRefer<T>
    finalized?: false
}): UnfinalizedDataSource<T>
export function source<T extends Data>(config: {
    reader: (offset?: DataRef<T>) => PromiseLike<DataReader<T>>
    ref: DataRefer<T>
    finalized: true
}): FinalizedDataSource<T>
export function source<T extends Data>(config: {
    reader: (offset?: DataRef<T>) => PromiseLike<DataReader<T>>
    ref: DataRefer<T>
    finalized?: boolean
}): FinalizedDataSource<T> | UnfinalizedDataSource<T>
export function source<T extends Data>(config: {
    reader: (offset?: DataRef<T>) => PromiseLike<DataReader<T>>
    ref: DataRefer<T>
    finalized?: boolean
}): FinalizedDataSource<T> | UnfinalizedDataSource<T> {
    return config.finalized === true
        ? finalizedSource({reader: config.reader, ref: config.ref})
        : unfinalizedSource({reader: config.reader, ref: config.ref})
}

function finalizedTarget<T extends Data>(config: {
    writer: () => PromiseLike<DataWriter<T> | UnfinalizedDataWriter<T>>
}): FinalizedDataTarget<T> {
    const {writer} = config
    let currentDataWriter: DataWriter<T> | undefined
    let currentOffset: DataRef<T> | undefined

    return {
        finalized: true,
        async head(): Promise<DataRef<T> | undefined> {
            if (!currentDataWriter) {
                currentDataWriter = await writer()
            }
            return currentOffset ?? currentDataWriter.offset
        },
        async write(batch: DataBatch<T>, ref: DataRefer<T>): Promise<void> {
            if (!currentDataWriter) {
                currentDataWriter = await writer()
                currentOffset = currentDataWriter.offset
            }
            await currentDataWriter.write(batch, ref)
            if (batch && batch.data.length > 0) {
                currentOffset = ref.get(last(batch.data))
            }
        },
        async fork(fork: DataFork<T>, ref: DataRefer<T>): Promise<DataRef<T> | undefined> {
            if (currentDataWriter) {
                return await currentDataWriter.fork?.(fork, ref)
            }
            return undefined
        },
        async close(): Promise<void> {
            await currentDataWriter?.close?.()
            currentDataWriter = undefined
        },
    }
}

function unfinalizedTarget<T extends Data>(config: {
    writer: () => PromiseLike<DataWriter<T> | UnfinalizedDataWriter<T>>
}): UnfinalizedDataTarget<T> {
    const {writer} = config
    let currentDataWriter: DataWriter<T> | undefined
    let currentOffset: DataRef<T> | undefined

    return {
        finalized: false,
        async head(): Promise<DataRef<T> | undefined> {
            if (!currentDataWriter) {
                currentDataWriter = await writer()
            }
            return currentOffset ?? currentDataWriter.offset
        },
        async write(batch: DataBatch<T>, ref: DataRefer<T>): Promise<void> {
            if (!currentDataWriter) {
                currentDataWriter = await writer()
                currentOffset = currentDataWriter.offset
            }
            await currentDataWriter.write(batch, ref)
            if (batch && batch.data.length > 0) {
                currentOffset = ref.get(last(batch.data))
            }
        },
        async fork(fork: DataFork<T>, ref: DataRefer<T>): Promise<DataRef<T> | undefined> {
            throw new ForkException(fork)
        },
        async close(): Promise<void> {
            await currentDataWriter?.close?.()
            currentDataWriter = undefined
        },
    }
}

export function target<T extends Data>(config: {
    writer: () => PromiseLike<UnfinalizedDataWriter<T>>
    finalized?: false
}): UnfinalizedDataTarget<T>
export function target<T extends Data>(config: {
    writer: () => PromiseLike<DataWriter<T>>
    finalized: true
}): FinalizedDataTarget<T>
export function target<T extends Data>(config: {
    writer: () => PromiseLike<DataWriter<T> | UnfinalizedDataWriter<T>>
    finalized?: boolean
}): DataTarget<T>
export function target<T extends Data>(config: {
    writer: () => PromiseLike<DataWriter<T> | UnfinalizedDataWriter<T>>
    finalized?: boolean
}): FinalizedDataTarget<T> | UnfinalizedDataTarget<T> {
    return config.finalized ? finalizedTarget(config) : unfinalizedTarget(config)
}

export async function pipe<T extends Data>(source: DataSource<T>, target: DataTarget<T>): Promise<void> {
    while (true) {
        const offset = await target.head()
        let batch: DataBatch<T> | null = null
        try {
            batch = await source.read(offset)
        } catch (err) {
            if (!target.finalized && isForkException<T>(err)) {
                await target.fork(err.fork, source.ref)
                continue
            }
            throw err
        }
        if (!batch) break
        await target.write(batch, source.ref)
    }
}

export interface UnfinalizedTransformerConfig<T extends Data, U extends Data> {
    transform: (batch: DataBatch<T>) => Promise<DataBatch<U>>
    fork: (fork: DataFork<T>, ref: DataRefer<T>) => Promise<DataFork<U>>
    flush?: () => Promise<DataBatch<U>>
    ref: DataRefer<U>
    finalized?: false
}

export interface FinalizedTransformerConfig<T extends Data, U extends Data> {
    transform: (batch: DataBatch<T>) => Promise<DataBatch<U>>
    fork?: (fork: DataFork<T>, ref: DataRefer<T>) => Promise<DataFork<U>>
    flush?: () => Promise<DataBatch<U>>
    ref: DataRefer<U>
    finalized: true
}

export function transformer<T extends Data, U extends Data>(
    config: UnfinalizedTransformerConfig<T, U>
): {target: UnfinalizedDataTarget<U>; source: UnfinalizedDataSource<T>}
export function transformer<T extends Data, U extends Data>(
    config: FinalizedTransformerConfig<T, U>
): {target: FinalizedDataTarget<U>; source: FinalizedDataSource<T>}
export function transformer<T extends Data, U extends Data>(
    config: UnfinalizedTransformerConfig<T, U> | FinalizedTransformerConfig<T, U>
): DataDuplex<T, U>
export function transformer<T extends Data, U extends Data>(
    config: UnfinalizedTransformerConfig<T, U> | FinalizedTransformerConfig<T, U>
): DataDuplex<T, U> {
    let dataFork: DataFork<U> | undefined = undefined

    let headFuture: Future<DataRef<U>> | undefined = undefined
    let dataFuture: Future<DataBatch<U>> = createFuture<DataBatch<U>>()
    let readyFuture: Future<void> = createFuture<void>()

    const targetInstance = target<T>({
        finalized: config.finalized,
        writer: async () => {
            const offset = await headFuture?.promise()
            return {
                offset,
                async write(batch: DataBatch<T>, ref: DataRefer<T>): Promise<void> {
                    await readyFuture.promise()
                    const result = await config.transform(batch)
                    dataFuture.resolve(result)
                    dataFuture = createFuture<DataBatch<U>>()
                },
                async fork(fork: DataFork<T>, ref: DataRefer<T>): Promise<DataRef<T> | undefined> {
                    dataFork = await config.fork?.(fork, ref)

                    headFuture?.resolve(undefined)
                    headFuture = createFuture<DataRef<U>>()

                    return headFuture.promise()
                },
                async close(): Promise<void> {
                    await sourceInstance.close()
                },
            }
        },
    })

    const sourceInstance = source<U>({
        finalized: config.finalized,
        reader: async (offset) => {
            headFuture?.resolve(offset)
            return {
                async read(): Promise<DataBatch<U> | null> {
                    if (dataFork) {
                        throw new ForkException(dataFork)
                    }

                    readyFuture.resolve()
                    readyFuture = createFuture<void>()

                    return await dataFuture.promise()
                },
                async close(): Promise<void> {
                    await targetInstance.close()
                },
            }
        },
        ref: config.ref,
    })

    return {
        target: targetInstance,
        source: sourceInstance,
    }
}
