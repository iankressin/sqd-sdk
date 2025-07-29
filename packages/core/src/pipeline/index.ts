import {last} from '../internal/misc'
import {isForkException} from './errors'
import type {
    Data,
    DataBatch,
    DataFork,
    DataReader,
    DataRef,
    DataRefer,
    DataWriter,
    UnfinalizedDataWriter,
    DataSource as DataSourceType,
    UnfinalizedDataSource,
    FinalizedDataSource,
    DataTarget as DataTargetType,
    UnfinalizedDataTarget,
    FinalizedDataTarget,
    DataDuplex,
} from './types'

export * from './types'
export * from './errors'

async function pipe<T extends Data>(source: DataSourceType<T>, target: DataTargetType<T>): Promise<void> {
    while (true) {
        const offset = await target.head()
        let batch: DataBatch<T> | undefined
        try {
            batch = await source.read(offset)
        } catch (err) {
            if (!isForkException<T>(err)) throw err
            if (!source.unfinalized) {
                throw new Error('Got fork exception from non-unfinalized source')
            }
            if (!target.unfinalized) {
                throw new Error('Got fork exception for non-unfinalized target')
            }
            await target.fork(err.fork)
            continue
        }
        if (!batch) break
        await target.write(batch)
    }
}

export interface FinalizedDataSourceConfig<T extends Data> {
    reader: (offset?: DataRef<T>) => PromiseLike<DataReader<T>>
    ref: DataRefer<T>
    unfinalized?: false
}

export interface UnfinalizedDataSourceConfig<T extends Data> {
    reader: (offset?: DataRef<T>) => PromiseLike<DataReader<T>>
    ref: DataRefer<T>
    unfinalized: true
}

class DataSource<T extends Data> {
    readonly unfinalized: boolean
    readonly ref: DataRefer<T>

    private _currentDataReader: DataReader<T> | undefined
    private _currentOffset: DataRef<T> | undefined

    constructor(config: FinalizedDataSourceConfig<T> | UnfinalizedDataSourceConfig<T>) {
        this.unfinalized = !!config.unfinalized
        this.ref = config.ref

        Object.assign(this, {
            _reader: config.reader,
        })
    }

    private _reader(offset?: DataRef<T>): Promise<DataReader<T>> {
        throw new Error('Not implemented')
    }

    async read(offset?: DataRef<T>): Promise<DataBatch<T>> {
        if (
            offset !== this._currentOffset &&
            (offset == null || this._currentOffset == null || this.ref.compare(offset, this._currentOffset) !== 'eq')
        ) {
            await this.close()
        }
        this._currentOffset = offset

        if (!this._currentDataReader) {
            this._currentDataReader = await this._reader(this._currentOffset)
        }

        const batch = await this._currentDataReader.read()
        if (batch && batch.data.length > 0) {
            this._currentOffset = this.ref.get(last(batch.data))
        }

        if (!batch.next) {
            await this.close()
        }

        return batch
    }

    pipeThrough<U extends DataSourceType<any>>(duplex: {target: DataTargetType<T>; source: U}): U {
        pipe(this, duplex.target).catch((err) => {
            throw err
        })
        return duplex.source
    }

    pipeTo(target: DataTargetType<T>): Promise<void> {
        return pipe(this, target)
    }

    async close(): Promise<void> {
        await this._currentDataReader?.close?.()
        this._currentDataReader = undefined
    }
}

export function source<T extends Data>(config: UnfinalizedDataSourceConfig<T>): UnfinalizedDataSource<T>
export function source<T extends Data>(config: FinalizedDataSourceConfig<T>): FinalizedDataSource<T>
export function source<T extends Data>(
    config: FinalizedDataSourceConfig<T> | UnfinalizedDataSourceConfig<T>
): DataSourceType<T>
export function source<T extends Data>(
    config: FinalizedDataSourceConfig<T> | UnfinalizedDataSourceConfig<T>
): DataSourceType<T> {
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

    private currentDataWriter: DataWriter<T> | UnfinalizedDataWriter<T> | undefined
    private currentOffset: DataRef<T> | undefined

    constructor(config: FinalizedDataTargetConfig<T> | UnfinalizedDataTargetConfig<T>) {
        this.unfinalized = !!config.unfinalized

        Object.assign(this, {
            _writer: config.writer,
        })

        if (this.unfinalized) {
            Object.assign(this, {
                fork: async (fork: DataFork<T>): Promise<DataRef<T> | undefined> => {
                    const dataWriter = await this.ensureDataWriter()
                    if (!dataWriter.fork) {
                        throw new TypeError('Missing fork method in unfinalized data writer')
                    }
                    this.currentOffset = await dataWriter.fork(fork)
                    return this.currentOffset
                },
            })
        }
    }

    private _writer(): Promise<DataWriter<T>> {
        throw new Error('Not implemented')
    }

    private async ensureDataWriter(): Promise<DataWriter<T>> {
        if (!this.currentDataWriter) {
            this.currentDataWriter = await this._writer()
        }
        return this.currentDataWriter
    }

    async head(): Promise<DataRef<T> | undefined> {
        const dataWriter = await this.ensureDataWriter()
        return this.currentOffset ?? dataWriter.offset
    }

    async write(batch: DataBatch<T>): Promise<void> {
        const dataWriter = await this.ensureDataWriter()
        this.currentOffset = this.currentOffset ?? dataWriter.offset
        await dataWriter.write(batch)
        if (batch && batch.data.length > 0) {
            this.currentOffset = batch.next
        }
    }

    async close(): Promise<void> {
        await this.currentDataWriter?.close?.()
        this.currentDataWriter = undefined
    }

    fork(_: DataFork<T>): Promise<DataRef<T> | undefined> {
        throw new Error('Not implemented')
    }
}

export function target<T extends Data>(config: UnfinalizedDataTargetConfig<T>): UnfinalizedDataTarget<T>
export function target<T extends Data>(config: FinalizedDataTargetConfig<T>): FinalizedDataTarget<T>
export function target<T extends Data>(
    config: FinalizedDataTargetConfig<T> | UnfinalizedDataTargetConfig<T>
): DataTargetType<T>
export function target<T extends Data>(
    config: FinalizedDataTargetConfig<T> | UnfinalizedDataTargetConfig<T>
): DataTargetType<T> {
    return new DataTarget(config)
}

export interface UnfinalizedTransformerConfig<T extends Data, U extends Data> {
    transform: (batch: DataBatch<T>) => Promise<DataBatch<U>>
    fork: (fork: DataFork<T>, ref: DataRefer<T>) => Promise<DataFork<U>>
    flush?: () => Promise<DataBatch<U>>
    ref: DataRefer<U>
    unfinalized: true
}

export interface FinalizedTransformerConfig<T extends Data, U extends Data> {
    transform: (batch: DataBatch<T>) => Promise<DataBatch<U>>
    fork?: (fork: DataFork<T>) => Promise<DataFork<U>>
    flush?: () => Promise<DataBatch<U>>
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
