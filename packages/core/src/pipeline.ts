import {createFuture, type Future} from './internal/async'
import {assert, last} from './internal/misc'
import type {Awaitable, Simplify} from './internal/types'

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
    readonly ref: DataRefer<D>
    readonly meta?: Record<string, any>
}

export interface DataFork<D extends Data = Data> {
    readonly heads: DataRef<D>[]
    readonly ref: DataRefer<D>
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

export class DataContinuityError extends Error {
    readonly isSqdDataContinuityError = true

    constructor() {
        super('Data continuity error')
    }
}

export interface DataSource<D extends Data, F extends boolean> {
    finalized: F
    init?: () => Awaitable<DataRef<D> | undefined>
    read: (offset?: DataRef<D>) => AsyncIterableIterator<DataBatch<D>>
    close?: () => Awaitable<void>
}

export class DataReader<D extends Data, F extends boolean> {
    private _head: DataRef<D> | undefined
    private _state?: 'idle' | 'reading' | 'closed'

    get finalized(): F {
        return this.source.finalized as F
    }

    constructor(readonly source: DataSource<D, F>) {}

    async head(): Promise<DataRef<D> | undefined> {
        await this._ensureInit()
        return this._head
    }

    async *read(offset?: DataRef<D>): AsyncIterableIterator<DataBatch<D>> {
        await this._ensureInit()

        if (this._state === 'closed') {
            throw new Error('DataReader is already closed')
        }

        if (this._state === 'reading') {
            throw new Error('DataReader is already reading')
        }

        try {
            this._state = 'reading'
            for await (const batch of this.source.read(offset)) {
                yield batch
                this._head = batch.head
            }
            this._state = 'closed'
        } catch (err) {
            this._state = 'idle'
            throw err
        }
    }

    async _ensureInit(): Promise<void> {
        if (this._state == null && this.source.init) {
            this._head = await this.source.init()
            this._state = 'idle'
        }
    }

    async pipeTo(writer: DataWriter<D, F extends true ? any : false>): Promise<void> {
        await this._ensureInit()
        return pipeTo(this, writer)
    }

    pipeThrough<T extends Data>(pair: DataWriterReaderPair<D, T, F extends true ? any : false>): typeof pair.reader {
        pipeTo(this, pair.writer).catch((e) => {
            throw e
        })

        return pair.reader
    }

    async close(): Promise<void> {
        await this._ensureInit()
        await this.source.close?.()
        this._state = 'closed'
    }

    [Symbol.asyncIterator](): AsyncIterator<DataBatch<D>> {
        return this.read({})
    }
}

interface DataSinkBase<D extends Data> {
    init?: () => Awaitable<DataRef<D> | undefined>
    write: (batch: DataBatch<D>, prevHead?: DataRef<D>) => Awaitable<void>
    fork?: (fork: DataFork<D>, prevHead?: DataRef<D>) => Awaitable<DataRef<D> | undefined>
    close?: () => Awaitable<void>
}

export interface FinalizedDataSink<D extends Data> extends DataSinkBase<D> {
    finalized: true
}

export interface UnfinalizedDataSink<D extends Data> extends DataSinkBase<D> {
    finalized?: false
    fork: (fork: DataFork<D>, prevHead?: DataRef<D>) => Awaitable<DataRef<D> | undefined>
}

export type DataSink<D extends Data, F extends boolean> = Extract<
    FinalizedDataSink<D> | UnfinalizedDataSink<D>,
    {finalized?: F}
>

export class DataWriter<D extends Data, F extends boolean> {
    #sink: DataSink<D, F>
    private _state?: 'idle' | 'writing' | 'closed'
    private _head: DataRef<D> | undefined

    readonly finalized?: F

    constructor(sink: DataSink<D, F>) {
        this.#sink = sink as DataSink<D, F>
        this.finalized = this.#sink.finalized
    }

    async head(): Promise<DataRef<D> | undefined> {
        await this._ensureInit()
        return this._head
    }

    private async _ensureInit(): Promise<void> {
        if (this._state == null && this.#sink.init) {
            this._head = await this.#sink.init()
            this._state = 'idle'
        }
    }

    async write(batch: DataBatch<D>): Promise<void> {
        await this._ensureInit()

        if (this._state === 'closed') {
            throw new Error('DataWriter is already closed')
        }

        if (this._state === 'writing') {
            throw new Error('DataWriter is already writing')
        }

        try {
            this._state = 'writing'
            assertContinuity(batch, this._head)
            await this.#sink.write(batch, this._head)
            this._head = batch.data.length > 0 ? batch.ref.get(last(batch.data)) : this._head
        } finally {
            this._state = 'idle'
        }
    }

    async fork(fork: DataFork<D>): Promise<void> {
        await this._ensureInit()

        if (this._state === 'closed') {
            throw new Error('DataWriter is already closed')
        }

        if (this._state === 'writing') {
            throw new Error('DataWriter is already writing')
        }

        if (this.#sink.fork == null) {
            throw new ForkException(fork)
        }

        try {
            this._state = 'writing'
            this._head = await this.#sink.fork(fork)
        } finally {
            this._state = 'idle'
        }
    }

    async close(): Promise<void> {
        await this._ensureInit()
        await this.#sink.close?.()
        this._state = 'closed'
    }
}

function assertContinuity<D extends Data>(batch: DataBatch<D>, head: DataRef<D> | undefined) {
    let last = head
    for (const item of batch.data) {
        const ref = batch.ref.get(item)
        if (last !== undefined && batch.ref.compare(last, ref) !== 'gt') {
            throw new DataContinuityError()
        }
        last = ref
    }
}

export interface DataWriterReaderPair<W extends Data, R extends Data, F extends boolean> {
    writer: DataWriter<W, F>
    reader: DataReader<R, F>
}

async function pipeTo<D extends Data, F extends boolean>(
    reader: DataReader<D, F>,
    writer: DataWriter<D, F extends true ? any : false>
): Promise<void> {
    try {
        while (true) {
            try {
                const offset = await writer.head()
                for await (const batch of reader.read(offset)) {
                    await writer.write(batch)
                }
            } catch (err) {
                if (isForkException<D>(err)) {
                    await writer.fork(err.fork)
                    continue
                }
                throw err
            }
        }
    } finally {
        await writer.close().catch(() => {})
    }
}

export type DataDuplexOptions<W extends Data, R extends Data, F extends boolean> = DataSink<W, F> & DataSource<R, F>

export class DataDuplex<W extends Data, R extends Data, F extends boolean> implements DataWriterReaderPair<W, R, F> {
    readonly writer: DataWriter<W, F>
    readonly reader: DataReader<R, F>

    private _readyFuture: Future<void> = createFuture()
    private _initFuture: Future<any | undefined> = createFuture()

    constructor(opts: DataDuplexOptions<W, R, F>) {
        this.writer = new DataWriter<W, F>({
            finalized: opts.finalized,
            init: () => this._initFuture.promise(),
            write: async (batch) => {
                await this._readyFuture.promise()
                await opts.write(batch)
            },
            fork: async (fork) => {
                await this._readyFuture.promise()
                await opts.fork(fork)
                this._dataFuture.resolve(fork)
                this._dataFuture = createFuture()
            },
            close: async () => {
                this._readyFuture.reject(new Error('DataDuplex closed'))
                this._initFuture.resolve(undefined)
            },
        })
        this.reader = new DataReader<R, F>({
            finalized: opts.finalized,
            read: async function* (offset) {
                while (true) {
                    readyFuture.resolve()

                    const batch = await dataFuture.promise()
                    if (batch === undefined) break

                    yield batch

                    readyFuture = createFuture()
                }
            },
            close: async () => {
                this._readyFuture.reject(new Error('BlockTransformer closed'))
                this._dataFuture.resolve(undefined)
                this._initFuture.resolve(undefined)
            },
        })
    }
}

export type FinalizedDataTransformer<W extends Data, R extends Data> = {
    finalized: true
    transform: (batch: DataBatch<W>) => Promise<DataBatch<R>>
    fork?: (fork: DataFork<W>) => Promise<DataFork<R>>
}
export type UnfinalizedDataTransformer<W extends Data, R extends Data> = {
    finalized?: false
    transform: (batch: DataBatch<W>) => Promise<DataBatch<R>>
    fork: (fork: DataFork<W>) => Promise<DataFork<R>>
}

export type DataTransformer<W extends Data, R extends Data, F extends boolean> = Simplify<
    (FinalizedDataTransformer<W, R> | UnfinalizedDataTransformer<W, R>) & {finalized: F}
>

export function transformer<W extends Data, R extends Data, F extends boolean>(
    opts: DataTransformer<W, R, F>
): DataWriterReaderPair<W, R, F> {
    let state: 'idle' | 'reading' | 'closed' = 'idle'

    let readyFuture: Future<void> = createFuture()
    let dataFuture: Future<DataBatch<R> | undefined> = createFuture()
    let initFuture: Future<any | undefined> = createFuture()

    return {
        writer: new DataWriter<W, F>({
            finalized: opts.finalized,
            write: async (batch) => {
                await readyFuture.promise()
                const transformedBatch = await opts.transform(batch)
                dataFuture.resolve(transformedBatch)
                dataFuture = createFuture()
            },
            fork:
                opts.fork != null
                    ? async (fork) => {
                          const forkError = new ForkException(fork)
                          readyFuture.reject(forkError)
                          dataFuture.reject(forkError)

                          state = 'idle'

                          initFuture = createFuture()
                          return await initFuture.promise()
                      }
                    : undefined,
            close: async () => {
                if (state === 'closed') return

                state = 'closed'
                readyFuture.reject(new Error('BlockTransformer closed'))
                dataFuture.resolve(undefined)
                initFuture.resolve(undefined)
            },
        }),
        reader: new DataReader<R, F>({
            finalized: opts.finalized,
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
        }),
    }
}

transformer({
    finalized: true,
    transform: async (batch) => batch,
    fork: async (fork) => fork,
}).writer.finalized

export interface DataFinalizerOptions {
    throwOnFork?: boolean
}

export function finalizer<D extends Data>(opts: DataFinalizerOptions): DataWriterReaderPair<D, D, true> {
    let state: 'idle' | 'reading' | 'closed' = 'idle'

    let readyFuture: Future<void> = createFuture()
    let dataFuture: Future<DataBatch<D> | undefined> = createFuture()
    let headFuture: Future<DataRef<D> | undefined> = createFuture()

    const buffer: DataItem<D>[] = []

    return {
        writer: new DataWriter<D, any>({
            finalized: false,
            init: () => headFuture.promise(),
            write: async (batch) => {
                await readyFuture.promise()
                buffer.push(...batch.data)
                if (batch.finalizedHead && batch.data.length > 0) {
                    const lastRef = batch.ref.get(last(batch.data))
                    const unfinalizedIndex =
                        batch.ref.compare(lastRef, batch.finalizedHead) === 'gt'
                            ? buffer.findIndex((block) => batch.ref.compare(batch.ref.get(block), lastRef) === 'gt')
                            : buffer.length - 1

                    if (unfinalizedIndex > 0) {
                        const data = buffer.splice(0, unfinalizedIndex)
                        dataFuture.resolve({
                            data,
                            finalizedHead: batch.finalizedHead,
                            head: batch.head,
                            ref: batch.ref,
                        })
                        dataFuture = createFuture()
                    }
                }
            },
            fork: async (fork) => {
                for (const head of fork.heads) {
                    if (buffer.length === 0) {
                        if (!opts.throwOnFork) break
                        throw new Error('unable to handle fork')
                    }

                    const lastBlock = last(buffer)
                    if (fork.ref.compare(lastBlock, head) === 'gt') continue
                    if (fork.ref.compare(lastBlock, head) === 'ls') buffer.pop()
                    if (fork.ref.compare(lastBlock, head) === 'eq') break
                }

                return buffer.length > 0 ? fork.ref.get(last(buffer)) : undefined
            },
            close: async () => {
                if (state === 'closed') return

                state = 'closed'
                readyFuture.reject(new Error('BlockFinalizer closed'))
                dataFuture.resolve(undefined)
                headFuture.resolve(undefined)
            },
        }),
        reader: new DataReader<D, any>({
            finalized: false,
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
        }),
    }
}
