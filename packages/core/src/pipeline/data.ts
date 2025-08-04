export type DataRef<T> = T & {
    compare(other: T): 'ls' | 'eq' | 'gt' | 'fk'
}

export interface Data<V = any, R = any> {
    value: V
    ref: DataRef<R>
}

export interface DataBatch<T extends Data> {
    readonly data: T[]
    readonly finalizedHead?: T['ref'] | undefined
    readonly head: T['ref']
    readonly offset: T['ref']
}

export interface DataFork<T extends Data> {
    readonly heads: T['ref'][]
}
