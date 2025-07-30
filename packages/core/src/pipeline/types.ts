export interface Data<I = unknown, R = unknown> {
    item: I
    ref: R
}

export type DataItem<D extends Data> = D['item']
export type DataRef<D extends Data> = D['ref']

export interface DataCursor<T extends Data> {
    get(ref: DataItem<T>): DataRef<T>
    compare(a: DataRef<T>, b: DataRef<T>): 'ls' | 'eq' | 'gt' | 'fk'
}

export interface DataBatch<T extends Data> {
    readonly data: DataItem<T>[]
    readonly finalizedHead?: DataRef<T> | undefined
    readonly head: DataRef<T>
    readonly offset: DataRef<T>
}

export interface DataFork<T extends Data> {
    readonly heads: DataRef<T>[]
}

export interface DataReaderOptions<T extends Data> {
    offset: DataRef<T> | undefined
}

export interface DataReader<T extends Data> {
    read(): Promise<DataBatch<T> | undefined>
    close?(): Promise<unknown>
}

export type DataSource<T extends Data> = FinalizedDataSource<T> | UnfinalizedDataSource<T>

export interface FinalizedDataSource<T extends Data> {
    readonly unfinalized: false
    readonly cursor: DataCursor<T>
    read(opts: DataReaderOptions<T>): AsyncIterable<DataBatch<T>>
    pipeThrough<U extends DataSource<any>>(duplex: {target: DataTarget<T>; source: U}): U
    pipeTo(target: DataTarget<T>): Promise<void>
    close(): Promise<void>
}

export interface UnfinalizedDataSource<T extends Data> {
    readonly unfinalized: true
    readonly cursor: DataCursor<T>
    read(opts: DataReaderOptions<T>): AsyncIterable<DataBatch<T>>
    pipeThrough<U extends DataSource<any>>(duplex: {target: UnfinalizedDataTarget<T>; source: U}): U
    pipeTo(target: UnfinalizedDataTarget<T>): Promise<void>
    close(): Promise<void>
}

export interface DataWriterOptions<T extends Data> {
    cursor: DataRef<T>
}

export interface FinalizedDataWriter<T extends Data> {
    offset: DataRef<T> | undefined
    write(batch: DataBatch<T>): Promise<unknown>
    fork?(fork: DataFork<T>): Promise<DataRef<T> | undefined>
    close?(): Promise<unknown>
}

export interface UnfinalizedDataWriter<T extends Data> extends FinalizedDataWriter<T> {
    fork(fork: DataFork<T>): Promise<DataRef<T> | undefined>
}

export type DataWriter<T extends Data> = FinalizedDataWriter<T> | UnfinalizedDataWriter<T>

export type DataTarget<T extends Data> = UnfinalizedDataTarget<T> | FinalizedDataTarget<T>

export interface UnfinalizedDataTarget<T extends Data> {
    unfinalized: true
    write(opts: DataWriterOptions<T>, read: (opts: DataReaderOptions<T>) => AsyncIterable<DataBatch<T>>): Promise<void>
    close(): Promise<void>
}

export interface FinalizedDataTarget<T extends Data> {
    unfinalized: false
    write(opts: DataWriterOptions<T>, read: (opts: DataReaderOptions<T>) => AsyncIterable<DataBatch<T>>): Promise<void>
    close(): Promise<void>
}

export interface DataDuplex<T extends Data, U extends Data> {
    target: DataTarget<T>
    source: DataSource<U>
}
