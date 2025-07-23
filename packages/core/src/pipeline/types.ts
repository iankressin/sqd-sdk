export interface Data<I = unknown, R = unknown> {
    item: I
    ref: R
}

export type DataItem<D extends Data> = D['item']
export type DataRef<D extends Data> = D['ref']

export interface DataRefer<T extends Data> {
    get(ref: DataItem<T>): DataRef<T>
    compare(a: DataRef<T>, b: DataRef<T>): 'ls' | 'eq' | 'gt' | 'fk'
}

export interface DataBatch<T extends Data> {
    readonly data: DataItem<T>[]
    readonly finalizedHead?: DataRef<T>
    readonly head: DataRef<T>
}

export interface DataFork<T extends Data> {
    readonly heads: DataRef<T>[]
}

export interface DataReader<T extends Data> {
    read(): PromiseLike<DataBatch<T> | null>
    close?(): PromiseLike<void>
}

export interface DataWriter<T extends Data> {
    offset?: DataRef<T>
    write(batch: DataBatch<T>, ref: DataRefer<T>): PromiseLike<void>
    fork?(fork: DataFork<T>, ref: DataRefer<T>): PromiseLike<DataRef<T> | undefined>
    close?(): PromiseLike<void>
}

export interface UnfinalizedDataWriter<T extends Data> extends DataWriter<T> {
    fork(fork: DataFork<T>, ref: DataRefer<T>): PromiseLike<DataRef<T> | undefined>
}

export namespace DataReader {
    export function fromAsync<T extends Data>(iterator: AsyncIterableIterator<DataBatch<T>>): DataReader<T> {
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

interface BaseDataSource<T extends Data> {
    ref: DataRefer<T>
    read(offset?: DataRef<T>): PromiseLike<DataBatch<T> | null>
    close(): PromiseLike<void>
}

interface BaseDataTarget<T extends Data> {
    head(): PromiseLike<DataRef<T> | undefined>
    write(batch: DataBatch<T>, ref: DataRefer<T>): PromiseLike<void>
    close(): PromiseLike<void>
}

export interface UnfinalizedDataSource<T extends Data> extends BaseDataSource<T> {
    finalized: false
    pipeThrough<U extends Data, F extends boolean>(duplex: {
        target: UnfinalizedDataTarget<T>
        source: DataSource<U, F>
    }): DataSource<U, F>
    pipeTo(target: UnfinalizedDataTarget<T>): PromiseLike<void>
}

export interface FinalizedDataSource<T extends Data> extends BaseDataSource<T> {
    finalized: true
    pipeThrough<U extends Data, F extends boolean>(duplex: {
        target: DataTarget<T>
        source: DataSource<U, F>
    }): DataSource<U, F>
    pipeTo(target: DataTarget<T>): PromiseLike<void>
}

export type DataSource<T extends Data, F extends boolean = any> = Extract<
    FinalizedDataSource<T> | UnfinalizedDataSource<T>,
    {finalized: F}
>

export interface FinalizedDataTarget<T extends Data> extends BaseDataTarget<T> {
    finalized: true
    fork?(fork: DataFork<T>, ref: DataRefer<T>): PromiseLike<DataRef<T> | undefined>
}

export interface UnfinalizedDataTarget<T extends Data> extends BaseDataTarget<T> {
    finalized: false
    fork(fork: DataFork<T>, ref: DataRefer<T>): PromiseLike<DataRef<T> | undefined>
}

export type DataTarget<T extends Data> = FinalizedDataTarget<T> | UnfinalizedDataTarget<T>

export interface DataDuplex<T extends Data, U extends Data> {
    target: DataTarget<T>
    source: DataSource<U>
}
