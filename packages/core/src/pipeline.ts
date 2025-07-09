import {createFuture, type Future} from './internal/async'
import {assert, last, maybeLast} from './internal/misc'

export interface DataSourceStreamOptions<R> {
    offset?: R
}

export interface Data<T, R> {
    item: T
    ref: R
}

export interface DataRef<D extends Data<any, any>> {
    get(ref: D['item']): D['ref']
    compare(a: D['ref'], b: D['ref']): DataRef.Compare
}

export namespace DataRef {
    export enum Compare {
        Less = 'less',
        Equal = 'equal',
        Greater = 'greater',
        Fork = 'fork',
    }
}

export interface DataBatchValue<D extends Data<any, any>, M extends Record<string, any> = Record<string, any>> {
    data: D['item'][]
    finalizedHead?: D['ref']
    head: D['ref']
    meta?: M
}

export class DataBatch<D extends Data<any, any>, M extends Record<string, any> = Record<string, any>>
    implements DataBatchValue<D, M>, Iterable<D['item']>
{
    constructor(readonly value: DataBatchValue<D, M>) {}

    get data(): D['item'][] {
        return this.value.data
    }

    get head(): D['ref'] {
        return this.value.head
    }

    get finalizedHead(): D['ref'] | undefined {
        return this.value.finalizedHead
    }

    get meta(): M | undefined {
        return this.value.meta
    }

    [Symbol.iterator](): Iterator<D['item']> {
        return this.data[Symbol.iterator]()
    }
}

export class ForkException<R = any> extends Error {
    readonly isSqdForkException = true

    constructor(public readonly refs: R[]) {
        assert(refs.length > 0)
        const lastRef = refs[refs.length - 1]
        super(`fork exception at ${lastRef}`)
    }

    override get name(): string {
        return 'ForkException'
    }
}

export const isForkException = (err: unknown): err is ForkException =>
    err instanceof Error && !!(err as Partial<ForkException>).isSqdForkException

export interface DataSource<D extends Data<any, any>> {
    init?(): Promise<D['ref'] | undefined>
    read(req: DataSourceStreamOptions<D['ref']>): AsyncIterable<DataBatchValue<D>>
    close?(): Promise<void>
}

export interface DataSink<D extends Data<any, any>> {
    init?(): Promise<D['ref'] | undefined>
    write(batch: DataBatch<D>, prev: D['ref']): Promise<D['ref'] | undefined>
    fork?(refs: D['ref'][]): Promise<D['ref'] | undefined>
    close?(): Promise<void>
}

export class DataReader<D extends Data<any, any>> {
    private _constructed = false
    protected _head: D['ref'] | undefined

    constructor(private readonly source: DataSource<D>, readonly ref: DataRef<D>) {}

    async head(): Promise<D['ref'] | undefined> {
        await this._ensureInit()
        return this._head
    }

    async *read(req: DataSourceStreamOptions<D['ref']>): AsyncIterable<DataBatch<D>> {
        await this._ensureInit()
        for await (const batch of this.source.read(req)) {
            this._head = batch.head
            yield batch instanceof DataBatch ? batch : new DataBatch(batch)
        }
    }

    async _ensureInit(): Promise<void> {
        if (!this._constructed && this.source.init) {
            this._head = await this.source.init()
            this._constructed = true
        }
    }

    async pipeTo(writer: DataWriter<D>): Promise<void> {
        await this._ensureInit()
        return pipeTo(this, writer)
    }

    pipeThrough<T extends Data<any, any>>(pair: DataWriterReaderPair<D, T>): typeof pair.reader {
        pipeTo(this, pair.writer).catch((e) => {
            throw e
        })

        return pair.reader
    }

    async close(): Promise<void> {
        await this._ensureInit()
        await this.source.close?.()
    }
}

export class DataWriter<D extends Data<any, any>> {
    private _constructed = false
    private _head: D['ref'] | undefined

    constructor(readonly sink: DataSink<D>, readonly ref: DataRef<D>) {}

    async head(): Promise<D['ref'] | undefined> {
        await this._ensureInit()
        return this._head
    }

    private async _ensureInit(): Promise<void> {
        if (!this._constructed && this.sink.init) {
            this._head = await this.sink.init()
            this._constructed = true
        }
    }

    async write(batch: DataBatch<D>): Promise<void> {
        await this._ensureInit()
        const head = await this.sink.write(batch, this._head)
        this._head = head ?? this._head
    }

    async fork(refs: D['ref'][]): Promise<void> {
        await this._ensureInit()

        if (this.sink.fork == null) {
            throw new ForkException(refs)
        }

        this._head = await this.sink.fork(refs)
    }
}

export interface DataWriterReaderPair<W extends Data<any, any>, R extends Data<any, any>> {
    writer: DataWriter<W>
    reader: DataReader<R>
}

async function pipeTo<D extends Data<any, any>>(reader: DataReader<D>, writer: DataWriter<D>): Promise<void> {
    try {
        while (true) {
            try {
                const head = await writer.head()

                for await (const batch of reader.read({offset: head})) {
                    await writer.write(batch)
                }

                break
            } catch (err) {
                if (isForkException(err)) {
                    await writer.fork(err.refs)
                    continue
                }
                throw err
            }
        }
    } finally {
        await writer.sink.close?.().catch(() => {})
    }
}

export function transformer<In extends Data<any, any>, Out extends Data<any, any>>(opts: {
    transform: (batch: DataBatch<In>) => Promise<DataBatch<Out>>
    refIn: DataRef<In>
    refOut: DataRef<Out>
}): DataWriterReaderPair<In, Out> {
    let state: 'idle' | 'reading' | 'closed' = 'idle'

    let readyFuture: Future<void> = createFuture()
    let dataFuture: Future<DataBatch<Out> | undefined> = createFuture()
    let initFuture: Future<any | undefined> = createFuture()

    return {
        writer: new DataWriter<In>(
            {
                init: async () => initFuture.promise(),
                write: async (batch) => {
                    await readyFuture.promise()
                    const transformedBatch = await opts.transform(batch)
                    dataFuture.resolve(transformedBatch)
                    dataFuture = createFuture()
                },
                fork: async (refs) => {
                    const forkError = new ForkException(refs)
                    readyFuture.reject(forkError)
                    dataFuture.reject(forkError)

                    state = 'idle'

                    initFuture = createFuture()
                    return await initFuture.promise()
                },
                close: async () => {
                    if (state === 'closed') return

                    state = 'closed'
                    readyFuture.reject(new Error('BlockTransformer closed'))
                    dataFuture.resolve(undefined)
                    initFuture.resolve(undefined)
                },
            },
            opts.refIn
        ),
        reader: new DataReader<Out>(
            {
                read: async function* (req) {
                    if (state === 'closed') return

                    if (state === 'reading') {
                        throw new Error('BlockTransformer is already read')
                    }

                    state = 'reading'
                    initFuture.resolve(req.offset)

                    while (true) {
                        if (state !== 'reading') break

                        readyFuture.resolve()

                        const batch = await dataFuture.promise()
                        if (batch === undefined) break

                        yield batch

                        readyFuture = createFuture()
                    }
                },
            },
            opts.refOut
        ),
    }
}

export interface DataFinalizerOptions<D extends Data<any, any>> {
    throwOnFork?: boolean
    ref: DataRef<D['ref']>
}

export function finalizer<D extends Data<any, any>>(opts: DataFinalizerOptions<D>): DataWriterReaderPair<D, D> {
    let state: 'idle' | 'reading' | 'closed' = 'idle'

    let readyFuture: Future<void> = createFuture()
    let dataFuture: Future<DataBatchValue<D> | undefined> = createFuture()
    let headFuture: Future<D['ref'] | undefined> = createFuture()

    const buffer: D['item'][] = []

    return {
        writer: new DataWriter<D>(
            {
                init: async () => headFuture.promise(),
                write: async ({data, finalizedHead}) => {
                    await readyFuture.promise()

                    const head = opts.ref.get(maybeLast(data))

                    buffer.push(...data)
                    if (finalizedHead) {
                        const unfinalizedIndex =
                            head && opts.ref.compare(head, finalizedHead) === DataRef.Compare.Greater
                                ? buffer.findIndex(
                                      (block) =>
                                          opts.ref.compare(opts.ref.get(block), finalizedHead) ===
                                          DataRef.Compare.Greater
                                  )
                                : buffer.length - 1

                        if (unfinalizedIndex > 0) {
                            const data = buffer.splice(0, unfinalizedIndex)
                            dataFuture.resolve({
                                data,
                                finalizedHead,
                                head: finalizedHead,
                            })
                            dataFuture = createFuture()
                        }
                    }

                    headFuture.resolve(head)
                },
                fork: async (refs) => {
                    for (const ref of refs) {
                        if (buffer.length === 0) {
                            if (!opts.throwOnFork) break
                            throw new Error('unable to handle fork')
                        }

                        const lastBlock = last(buffer)
                        if (lastBlock.ref < ref) continue
                        if (lastBlock.ref > ref) buffer.pop()
                        if (lastBlock.ref === ref) break
                    }

                    return undefined
                },
                close: async () => {
                    if (state === 'closed') return

                    state = 'closed'
                    readyFuture.reject(new Error('BlockFinalizer closed'))
                    dataFuture.resolve(undefined)
                    headFuture.resolve(undefined)
                },
            },
            opts.ref
        ),
        reader: new DataReader<D>(
            {
                read: async function* (req) {
                    if (state === 'closed') return

                    if (state === 'reading') {
                        throw new Error('BlockFinalizer is already read')
                    }

                    state = 'reading'
                    headFuture.resolve(req.offset)

                    while (true) {
                        if (state !== 'reading') break

                        readyFuture.resolve()

                        const batch = await dataFuture.promise()
                        if (batch === undefined) break

                        yield batch

                        readyFuture = createFuture()
                    }
                },
            },
            opts.ref
        ),
    }
}
