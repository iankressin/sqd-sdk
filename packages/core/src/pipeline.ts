import {createFuture, type Future} from './internal/async'
import {assert, last, maybeLast} from './internal/misc'
import type {Range} from './internal/range'

export interface BlockRef {
    number: number
    hash?: string
}

export interface BlockBase {
    header: BlockRef
}

export interface DataSourceStreamOptions {
    range?: Range
    parentHash?: string
}

export interface BlockBatch<B extends BlockBase> {
    data: B[]
    finalizedHead?: BlockRef
    head: BlockRef
}

export class ForkException extends Error {
    readonly isSqdForkException = true

    constructor(public readonly previousBlocks: BlockRef[]) {
        assert(previousBlocks.length > 0)
        const lastRef = previousBlocks[previousBlocks.length - 1]
        super(`expected ${lastRef.number + 1} to have parent ${lastRef.number}#${lastRef.hash}`)
    }

    override get name(): string {
        return 'ForkException'
    }
}

export const isForkException = (err: unknown): err is ForkException =>
    err instanceof Error && !!(err as Partial<ForkException>).isSqdForkException

export class DataConsistencyError extends Error {
    readonly isSqdDataConsistencyError = true
}

export class BlockConsistencyError extends DataConsistencyError {
    constructor(ref: BlockRef, errorMsg?: string) {
        super(`Failed to fetch block ${ref.number}#${ref.hash}${errorMsg ? `: ${errorMsg}` : ''}`)
    }
}

export const isDataConsistencyError = (err: unknown): err is Error =>
    err instanceof Error && !!(err as Partial<DataConsistencyError>).isSqdDataConsistencyError

export interface BlockSource<B extends BlockBase> {
    init?(): Promise<BlockRef | undefined>
    read(req: DataSourceStreamOptions): AsyncIterable<BlockBatch<B>>
    close?(): Promise<void>
}

export interface BlockSink<B extends BlockBase> {
    init?(): Promise<BlockRef | undefined>
    write(batch: BlockBatch<B>): Promise<void>
    fork?(refs: BlockRef[], head?: BlockRef): Promise<BlockRef | undefined>
    close?(): Promise<void>
}

export class BlockReader<B extends BlockBase> {
    private _constructed = false
    protected _head: BlockRef | undefined

    constructor(public readonly source: BlockSource<B>) {}

    async head(): Promise<BlockRef | undefined> {
        await this._ensureConstructed()
        return this._head
    }

    async *read(req: DataSourceStreamOptions): AsyncIterable<BlockBatch<B>> {
        await this._ensureConstructed()
        for await (const batch of this.source.read(req)) {
            this._head = batch.head
            yield batch
        }
    }

    async _ensureConstructed(): Promise<void> {
        if (!this._constructed && this.source.init) {
            this._head = await this.source.init()
            this._constructed = true
        }
    }

    async pipeTo(writer: BlockWriter<B>): Promise<void> {
        await this._ensureConstructed()
        return pipeTo(this, writer)
    }

    pipeThrough<T extends BlockBase>(pair: BlockWriterReaderPair<B, T>): BlockReader<T> {
        pipeTo(this, pair.writer).catch((e) => {
            throw e
        })

        return pair.reader
    }

    async close(): Promise<void> {
        await this._ensureConstructed()
        await this.source.close?.()
    }
}

export interface BlockWriterReaderPair<W extends BlockBase, R extends BlockBase> {
    writer: BlockWriter<W>
    reader: BlockReader<R>
}

export class BlockWriter<B extends BlockBase> {
    private _constructed = false
    private _head: BlockRef | undefined

    constructor(public readonly sink: BlockSink<B>) {}

    async head(): Promise<BlockRef | undefined> {
        await this._ensureConstructed()
        return this._head
    }

    private async _ensureConstructed(): Promise<void> {
        if (!this._constructed && this.sink.init) {
            this._head = await this.sink.init()
            this._constructed = true
        }
    }

    async write(batch: BlockBatch<B>): Promise<void> {
        await this._ensureConstructed()
        await this.sink.write(batch)
        if (batch.data.length > 0) {
            this._head = last(batch.data).header
        }
    }

    async fork(refs: BlockRef[]): Promise<void> {
        await this._ensureConstructed()

        if (this.sink.fork == null) {
            throw new ForkException(refs)
        }

        this._head = await this.sink.fork(refs)
    }
}

async function pipeTo<B extends BlockBase>(reader: BlockReader<B>, writer: BlockWriter<B>): Promise<void> {
    // Ensure both reader and writer are constructed

    try {
        while (true) {
            try {
                const head = await writer.head()
                const startFrom = head?.number ? head.number + 1 : 0

                // Read from source and write chunks one by one
                for await (const batch of reader.source.read({
                    range: {from: startFrom},
                    parentHash: head?.hash,
                })) {
                    await writer.write(batch)
                }

                // Source exhausted - normal completion
                break
            } catch (err) {
                if (isForkException(err)) {
                    // Fork exceptions restart the pipeline from new head
                    await writer.fork(err.previousBlocks)
                    continue
                }
                // Other errors propagate up
                throw err
            }
        }
    } finally {
        // Always cleanup, regardless of how we exit
        await writer.sink.close?.().catch(() => {})
    }
}

export function transformer<In extends BlockBase, Out extends BlockBase>(
    transform: (batch: BlockBatch<In>) => BlockBatch<Out> | Promise<BlockBatch<Out>>,
): BlockWriterReaderPair<In, Out> {
    let state: 'idle' | 'reading' | 'closed' = 'idle'

    let readyFuture: Future<void> = createFuture()
    let dataFuture: Future<BlockBatch<Out> | undefined> = createFuture()
    let initFuture: Future<BlockRef | undefined> = createFuture()

    return {
        writer: new BlockWriter<In>({
            init: async () => initFuture.promise(),
            write: async (batch) => {
                await readyFuture.promise()
                const transformedBatch = await transform(batch)
                dataFuture.resolve(transformedBatch)
                dataFuture = createFuture()
            },
            fork: async (refs, head) => {
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
        }),
        reader: new BlockReader<Out>({
            read: async function* (req) {
                if (state === 'closed') return

                if (state === 'reading') {
                    throw new Error('BlockTransformer is already read')
                }

                state = 'reading'
                initFuture.resolve(req.range ? {number: req.range.from - 1, hash: req.parentHash} : undefined)

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

export interface BlockFinalizerOptions {
    finalityConfirmation?: number
    throwOnFork?: boolean
}

export function finalizer<B extends BlockBase>(opts: BlockFinalizerOptions = {}): BlockWriterReaderPair<B, B> {
    let head: BlockRef | undefined
    let state: 'idle' | 'reading' | 'closed' = 'idle'

    let readyFuture: Future<void> = createFuture()
    let dataFuture: Future<BlockBatch<B> | undefined> = createFuture()
    let headFuture: Future<BlockRef | undefined> = createFuture()

    const buffer: B[] = []

    return {
        writer: new BlockWriter<B>({
            init: async () => head ?? headFuture.promise(),
            write: async (batch) => {
                await readyFuture.promise()

                buffer.push(...batch.data)
                const lastBlock = maybeLast(buffer)

                if (opts.finalityConfirmation || batch.finalizedHead) {
                    const finalizedNumber = opts.finalityConfirmation
                        ? (batch.head ?? last(buffer).header).number - opts.finalityConfirmation
                        : batch.finalizedHead!.number
                    const unfinalizedIndex = buffer.findIndex((block) => block.header.number > finalizedNumber)
                    if (unfinalizedIndex > 0) {
                        const data = buffer.splice(0, unfinalizedIndex)
                        dataFuture.resolve({
                            data,
                            finalizedHead: batch.finalizedHead,
                            head: last(data).header,
                        })
                        dataFuture = createFuture()
                    }
                }

                if (lastBlock) {
                    head = lastBlock.header
                }
            },
            fork: async (refs) => {
                for (const ref of refs) {
                    if (buffer.length === 0) {
                        if (!opts.throwOnFork) break
                        throw new Error('unable to handle fork')
                    }

                    const lastBlock = last(buffer)
                    if (lastBlock.header.number < ref.number) continue
                    if (lastBlock.header.number > ref.number) buffer.pop()
                    if (lastBlock.header.number === ref.number && lastBlock.header.hash !== ref.hash) buffer.pop()
                    if (lastBlock.header.number === ref.number && lastBlock.header.hash === ref.hash) break
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
        }),
        reader: new BlockReader<B>({
            init: async () => head,
            read: async function* (req) {
                if (state === 'closed') return

                if (state === 'reading') {
                    throw new Error('BlockFinalizer is already read')
                }

                state = 'reading'
                headFuture.resolve(req.range ? {number: req.range.from - 1, hash: req.parentHash} : undefined)

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

export interface BlockBatcherOptions<B extends BlockBase> {
    minSize?: number
    maxSize?: number
    size?: (batch: BlockBatch<B>) => number
    capacity?: number
}

export function batcher<B extends BlockBase>(opts: BlockBatcherOptions<B>): BlockWriterReaderPair<B, B> {
    const {minSize = 1, maxSize = 100, size = (batch) => batch.data.length, capacity = 1000} = opts

    let head: BlockRef | undefined
    let state: 'idle' | 'reading' | 'closed' = 'idle'
    let readyFuture: Future<void> = createFuture()
    let dataFuture: Future<BlockBatch<B> | undefined> = createFuture()
    let headFuture: Future<BlockRef | undefined> = createFuture()

    const buffer: B[] = []
    let currentSize = 0

    const flushBuffer = () => {
        if (buffer.length === 0) return

        const data = buffer.splice(0)
        const batch: BlockBatch<B> = {
            data,
            head: last(data).header,
        }
        currentSize = 0
        dataFuture.resolve(batch)
        dataFuture = createFuture()
    }

    return {
        writer: new BlockWriter<B>({
            init: async () => head ?? headFuture.promise(),
            write: async (batch) => {
                await readyFuture.promise()

                for (const block of batch.data) {
                    if (currentSize >= capacity) {
                        flushBuffer()
                        await readyFuture.promise()
                    }

                    buffer.push(block)
                    currentSize += size({data: [block], head: block.header})
                    head = block.header

                    if (buffer.length >= maxSize || currentSize >= capacity) {
                        flushBuffer()
                        await readyFuture.promise()
                    }
                }

                if (batch.finalizedHead) {
                    flushBuffer()
                }
            },
            fork: async (refs) => {
                buffer.length = 0
                currentSize = 0
                const forkError = new ForkException(refs)
                readyFuture.reject(forkError)
                dataFuture.reject(forkError)

                state = 'idle'
                head = undefined
                headFuture.resolve(undefined)
                headFuture = createFuture()

                return undefined
            },
            close: async () => {
                if (state === 'closed') return

                flushBuffer()
                state = 'closed'
                readyFuture.reject(new Error('BlockBatcher closed'))
                dataFuture.resolve(undefined)
                headFuture.resolve(undefined)
            },
        }),
        reader: new BlockReader<B>({
            init: async () => head,
            read: async function* (req) {
                if (state === 'closed') return

                if (state === 'reading') {
                    throw new Error('BlockBatcher is already read')
                }

                state = 'reading'
                headFuture.resolve(req.range ? {number: req.range.from - 1, hash: req.parentHash} : undefined)

                while (true) {
                    if (state !== 'reading') break

                    readyFuture.resolve()

                    const batch = await dataFuture.promise()
                    if (batch === undefined) break

                    if (batch.data.length >= minSize) {
                        yield batch
                    }

                    readyFuture = createFuture()
                }
            },
        }),
    }
}
