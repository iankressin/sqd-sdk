export interface Data<I = any, R = any> {
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
    readonly cursor: DataCursor<T>
}

export interface DataFork<T extends Data> {
    readonly heads: DataRef<T>[]
    readonly cursor: DataCursor<T>
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
    read(opts: DataReaderOptions<T>): AsyncIterable<DataBatch<T>>
    pipeThrough<U extends DataSource<any>>(duplex: PipableThrough<{target: DataTarget<T>; source: U}>): U
    pipeTo(target: DataTarget<T>): Promise<void>
    close(): Promise<void>
}

export interface UnfinalizedDataSource<T extends Data> {
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
    offset: DataRef<T> | undefined
    write(batch: DataBatch<T>, offset: DataRef<T>): Promise<DataRef<T>>
    fork?(fork: DataFork<T>, offset: DataRef<T>): Promise<DataRef<T> | undefined>
    close?(): Promise<unknown>
}

export interface UnfinalizedDataWriter<T extends Data> extends FinalizedDataWriter<T> {
    fork(fork: DataFork<T>, offset: DataRef<T>): Promise<DataRef<T> | undefined>
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
