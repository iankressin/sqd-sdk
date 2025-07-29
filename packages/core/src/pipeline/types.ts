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
    readonly finalizedHead?: DataRef<T> | undefined
    readonly head: DataRef<T>
    readonly next?: DataRef<T> | undefined
}

export interface DataFork<T extends Data> {
    readonly heads: DataRef<T>[]
}

export interface DataReader<T extends Data> {
    read(): PromiseLike<DataBatch<T>>
    close?(): PromiseLike<unknown>
}

export interface DataWriter<T extends Data> {
    offset?: DataRef<T>
    write(batch: DataBatch<T>): PromiseLike<unknown>
    fork?(fork: DataFork<T>): PromiseLike<DataRef<T> | undefined>
    close?(): PromiseLike<unknown>
}

export interface UnfinalizedDataWriter<T extends Data> extends DataWriter<T> {
    fork(fork: DataFork<T>): PromiseLike<DataRef<T> | undefined>
}

export type DataSource<T extends Data> = FinalizedDataSource<T> | UnfinalizedDataSource<T>

export interface FinalizedDataSource<T extends Data> {
    readonly unfinalized: false
    read(offset?: DataRef<T>): PromiseLike<DataBatch<T>>
    pipeThrough<U extends DataSource<any>>(duplex: {target: DataTarget<T>; source: U}): U
    pipeTo(target: DataTarget<T>): PromiseLike<void>
    close(): PromiseLike<void>
}

export interface UnfinalizedDataSource<T extends Data> {
    readonly unfinalized: true
    read(offset?: DataRef<T>): PromiseLike<DataBatch<T>>
    pipeThrough<U extends DataSource<any>>(duplex: {target: UnfinalizedDataTarget<T>; source: U}): U
    pipeTo(target: UnfinalizedDataTarget<T>): PromiseLike<void>
    close(): PromiseLike<void>
}

export type DataTarget<T extends Data> = UnfinalizedDataTarget<T> | FinalizedDataTarget<T>

export interface UnfinalizedDataTarget<T extends Data> {
    unfinalized: true
    head(): PromiseLike<DataRef<T> | undefined>
    write(batch: DataBatch<T>): PromiseLike<void>
    close(): PromiseLike<void>
    fork(fork: DataFork<T>): PromiseLike<DataRef<T> | undefined>
}

export interface FinalizedDataTarget<T extends Data> {
    unfinalized: false
    head(): PromiseLike<DataRef<T> | undefined>
    write(batch: DataBatch<T>): PromiseLike<void>
    close(): PromiseLike<void>
}

export interface DataDuplex<T extends Data, U extends Data> {
    target: DataTarget<T>
    source: DataSource<U>
}
