import {assert, last} from './internal/misc'

export interface Data<I = unknown, R = unknown> {
    item: I
    ref: R
}

export type DataItem<D extends Data> = D['item']
export type DataRef<D extends Data> = D['ref']

export interface DataRefer<D extends Data = Data> {
    get(ref: DataItem<D>): DataRef<D>
    compare(a: DataRef<D>, b: DataRef<D>): 'ls' | 'eq' | 'gt' | 'fk'
}

export interface DataBatch<D extends Data = Data> {
    readonly data: DataItem<D>[]
    readonly finalizedHead?: DataRef<D>
    readonly head: DataRef<D>
}

export interface DataFork<D extends Data = Data> {
    readonly heads: DataRef<D>[]
}

export interface DataReader<T extends Data> {
    read(): Promise<DataBatch<T> | null>
    close(): Promise<void>
}

export interface DataWriter<T extends Data> {
    offset?: DataRef<T>
    write(batch: DataBatch<T>): Promise<void>
    fork(fork: DataFork<T>): Promise<DataRef<T> | undefined>
    close(): Promise<void>
}

export namespace DataReader {
    export function fromAsync<T extends Data>(iterator: AsyncIterator<DataBatch<T>>): DataReader<T> {
        return {
            async read(): Promise<DataBatch<T> | null> {
                const result = await iterator.next()
                return result.done ? null : result.value
            },
            async close(): Promise<void> {
                await iterator.return?.()
            },
        }
    }
}

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

export const isForkException = <D extends Data = Data>(err: unknown): err is ForkException<D> =>
    err instanceof Error && !!(err as Partial<ForkException<D>>).isSqdForkException

export interface DataSource<T extends Data> {
    ref: DataRefer<T>
    read(offset?: DataRef<T>): Promise<DataBatch<T> | null>
    close(): Promise<void>
    pipeThrough<U extends Data>(duplex: DataDuplex<T, U>): DataSource<U>
    pipeTo(target: DataTarget<T>): Promise<void>
}

export interface DataTarget<T extends Data> {
    ref: DataRefer<T>
    write(batch: DataBatch<T>): Promise<void>
    fork(fork: DataFork<T>): Promise<DataRef<T> | undefined>
    close(): Promise<void>
}

export interface DataDuplex<T extends Data, U extends Data> {
    target: DataTarget<T>
    source: DataSource<U>
}

export interface DataSourceOptions<T extends Data, F extends boolean> {
    reader: (offset?: DataRef<T>) => DataReader<T>
    ref: DataRefer<T>
    finalized?: F
}

export function source<T extends Data, F extends boolean>(options: DataSourceOptions<T, F>): DataSource<T> {
    const {reader, ref, finalized} = options

    let currentDataReader: DataReader<T> | undefined
    let currentOffset: DataRef<T> | undefined

    return {
        ref,
        async read(offset?: DataRef<T>): Promise<DataBatch<T> | null> {
            if (ref.compare(offset, currentOffset) !== 'gt') {
                await currentDataReader?.close()
                currentDataReader = undefined
                currentOffset = offset
            }

            if (!currentDataReader) {
                currentDataReader = reader(currentOffset)
            }

            const batch = await currentDataReader.read()
            if (batch && batch.data.length > 0) {
                currentOffset = ref.get(last(batch.data))
            }

            return batch
        },
        async close(): Promise<void> {
            await currentDataReader?.close()
            currentDataReader = undefined
        },
        pipeThrough<U extends Data>(duplex: DataDuplex<T, U>): DataSource<U> {
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

export interface DataTargetOptions<T extends Data, F extends boolean> {
    writer: () => DataWriter<T>
    ref: DataRefer<T>
    finalized?: F
}

export function target<T extends Data, F extends boolean>(options: DataTargetOptions<T, F>): DataTarget<T> {
    const {writer, ref, finalized} = options

    return {
        ref,
        async write(batch: DataBatch<T>): Promise<void> {
            const dataWriter = writer()
            await dataWriter.write(batch)
        },
        async fork(fork: DataFork<T>): Promise<DataRef<T> | undefined> {
            const dataWriter = writer()
            return await dataWriter.fork(fork)
        },
        async close(): Promise<void> {
            const dataWriter = writer()
            await dataWriter.close()
        },
    }
}

export function transformer<T extends Data, U extends Data>(
    fn: (batch: DataBatch<T>) => Promise<DataBatch<U>>,
    targetRef: DataRefer<T>,
    sourceRef: DataRefer<U>
): DataDuplex<T, U> {
    const buffer: DataBatch<T>[] = []
    let closed = false

    const target: DataTarget<T> = {
        ref: targetRef,
        async write(batch: DataBatch<T>): Promise<void> {
            if (!closed) {
                buffer.push(batch)
            }
        },
        async fork(fork: DataFork<T>): Promise<DataRef<T> | undefined> {
            // Handle fork logic here
            return undefined
        },
        async close(): Promise<void> {
            closed = true
        },
    }

    const source: DataSource<U> = {
        ref: sourceRef,
        async read(offset?: DataRef<U>): Promise<DataBatch<U> | null> {
            if (buffer.length === 0) {
                return null
            }
            const batch = buffer.shift()!
            return await fn(batch)
        },
        async close(): Promise<void> {
            closed = true
        },
        pipeThrough<V extends Data>(duplex: DataDuplex<U, V>): DataSource<V> {
            pipe(this, duplex.target).catch((e) => {
                throw e
            })
            return duplex.source
        },
        pipeTo(target: DataTarget<U>): Promise<void> {
            return pipe(this, target)
        },
    }

    return {target, source}
}

export function finalizer<T extends Data>(ref: DataRefer<T>, options?: {throwOnFork?: boolean}): DataDuplex<T, T> {
    const buffer: DataItem<T>[] = []

    return transformer<T, T>(
        async (batch) => {
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

            // Return empty batch if nothing to finalize yet
            return {
                data: [],
                head: batch.head,
            }
        },
        ref,
        ref
    )
}

export async function pipe<T extends Data>(source: DataSource<T>, target: DataTarget<T>): Promise<void> {
    let offset: DataRef<T> | undefined

    while (true) {
        try {
            let batch: DataBatch<T> | null = await source.read(offset)
            // Clear offset after first read
            offset = undefined

            while (batch !== null) {
                await target.write(batch)
                batch = await source.read()
            }
            break
        } catch (err) {
            if (isForkException(err)) {
                offset = await target.fork(err.fork as DataFork<T>)
                if (offset !== undefined) {
                    continue
                }
            }
            throw err
        }
    }
}
