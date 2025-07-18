import {createFuture, type Future} from '../internal/async'
import {last} from '../internal/misc'
import {ForkException, isForkException} from './errors'
import type {
    Data,
    DataFork,
    DataRefer,
    DataRef,
    DataBatch,
    DataReader,
    DataWriter,
    DataItem,
    DataDuplex,
    DataSource,
    DataTarget,
    UnfinalizedDataWriter,
    Finalization,
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

export type DataSourceConfig<T extends Data, F extends boolean> = (
    | FinalizedDataSourceConfig<T>
    | UnfinalizedDataSourceConfig<T>
) & {finalized?: F}

export function source<T extends Data, F extends boolean = false>(options: DataSourceConfig<T, F>): DataSource<T, F> {
    const {reader, ref, finalized = false} = options

    let currentDataReader: DataReader<T> | undefined
    let currentOffset: DataRef<T> | undefined

    return {
        ref,
        finalized,
        async read(offset) {
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
        pipeThrough<U extends Data>(duplex: DataDuplex<T, U>) {
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

let a = source({
    reader: async () => {
        return {
            read: async () => null,
        }
    },
    ref: {
        get: () => {},
        compare: () => 'eq',
    },
}).finalized

export interface UnfinalizedDataTargetConfig<T extends Data> {
    writer: () => PromiseLike<UnfinalizedDataWriter<T>>
    finalized?: boolean
}

export interface FinalizedDataTargetConfig<T extends Data> {
    writer: () => PromiseLike<DataWriter<T>>
    finalized: true
}

export type DataTargetConfig<T extends Data, F extends boolean> = (
    | FinalizedDataTargetConfig<T>
    | UnfinalizedDataTargetConfig<T>
) & {finalized?: F}

export function target<T extends Data, F extends boolean>(options: DataTargetConfig<T, F>): DataTarget<T, F> {
    const {writer, finalized} = options
    let currentDataWriter: DataWriter<T> | undefined
    let currentOffset: DataRef<T> | undefined

    return {
        finalized: finalized ?? false,
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
            if (!this.finalized) {
                // TODO: should we throw ForkException or just an error?
                throw new ForkException(fork)
            }
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
): DataDuplex<T, U>
export function transformer<T extends Data, U extends Data>(config: FinalizedTransformerConfig<T, U>): DataDuplex<T, U>
export function transformer<T extends Data, U extends Data>(
    config: UnfinalizedTransformerConfig<T, U> | FinalizedTransformerConfig<T, U>
): DataDuplex<T, U> {
    let dataFork: DataFork<U> | undefined = undefined

    let headFuture: Future<DataRef<U>> | undefined = undefined
    let dataFuture: Future<DataBatch<U>> = createFuture<DataBatch<U>>()
    let readyFuture: Future<void> = createFuture<void>()

    const targetInstance = target<T>({
        finalized: config.finalized ?? false,
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
        finalized: config.finalized ?? false,
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

export function finalizer<T extends Data>(ref: DataRefer<T>, options?: {throwOnFork?: boolean}): DataDuplex<T, T> {
    const buffer: DataItem<T>[] = []

    return transformer<T, T>({
        transform: async (batch) => {
            buffer.push(...batch.data)

            if (batch.finalizedHead && batch.data.length > 0) {
                const lastRef = ref.get(last(batch.data))
                const unfinalizedIndex =
                    ref.compare(lastRef, batch.finalizedHead) === 'gt'
                        ? buffer.findIndex((item) => ref.compare(ref.get(item), batch.finalizedHead!) === 'gt')
                        : buffer.length

                if (unfinalizedIndex > 0) {
                    const data = buffer.splice(0, unfinalizedIndex)
                    return {
                        data,
                        finalizedHead: batch.finalizedHead,
                        head: batch.head,
                    }
                }
            }

            return {
                data: [],
                head: batch.head,
            }
        },
        fork: async (fork) => {
            return fork
        },
        ref,
    })
}
