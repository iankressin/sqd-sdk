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
    blocks: B[]
    finalizedHead?: BlockRef
    head?: BlockRef
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
    head(): Promise<BlockRef | undefined>
    read(req: DataSourceStreamOptions): AsyncIterable<BlockBatch<B>>
}

export interface BlockSink<B extends BlockBase> {
    head(): Promise<BlockRef | undefined>
    write(batch: BlockBatch<B>): Promise<void>
    close(): Promise<void>
    fork?(refs: BlockRef[]): Promise<void>
}

export class BlockReader<B extends BlockBase> {
    constructor(private readonly source: BlockSource<B>) {}

    async *read(req: DataSourceStreamOptions): AsyncIterable<BlockBatch<B>> {
        for await (const batch of this.source.read(req)) {
            yield batch
        }
    }

    pipeTo(writer: BlockWriter<B>): Promise<void> {
        return writer.write(this)
    }

    pipeThrough<T extends BlockBase>(pair: BlockWriterReaderPair<B, T>): BlockReader<T> & {done: Promise<void>} {
        const done = pair.writer.write(this)
        Object.defineProperty(pair.reader, 'done', {value: done, enumerable: false})
        return pair.reader as BlockReader<T> & {done: Promise<void>}
    }
}

export interface BlockWriterReaderPair<W extends BlockBase, R extends BlockBase> {
    writer: BlockWriter<W>
    reader: BlockReader<R>
}

export interface BlockWriterOptions {
    range?: Range
}

export class BlockWriter<B extends BlockBase> {
    constructor(
        private readonly sink: BlockSink<B>,
        private readonly opts: BlockWriterOptions = {},
    ) {}

    head(): Promise<BlockRef | undefined> {
        return this.sink.head()
    }

    async write(reader: BlockReader<B>): Promise<void> {
        while (true) {
            const head = await this.sink.head()
            const {from, parentHash} = head
                ? this.opts.range
                    ? head.number < this.opts.range.from
                        ? {from: this.opts.range.from, parentHash: undefined}
                        : {from: head.number + 1, parentHash: head.hash}
                    : {from: head.number + 1, parentHash: head.hash}
                : {from: this.opts.range?.from ?? 0, parentHash: undefined}

            try {
                for await (const batch of reader.read({range: {from, to: this.opts.range?.to}, parentHash})) {
                    await this.sink.write(batch)
                }
                return this.sink.close()
            } catch (err) {
                if (isForkException(err) && this.sink.fork) {
                    await this.sink.fork(err.previousBlocks)
                    continue
                }
                throw err
            }
        }
    }
}

export function transformer<In extends BlockBase, Out extends BlockBase>(
    transform: (batch: BlockBatch<In>) => BlockBatch<Out> | Promise<BlockBatch<Out>>,
): BlockWriterReaderPair<In, Out> {
    let head: BlockRef | undefined
    let state: 'idle' | 'reading' | 'closed' = 'idle'

    let readyFuture: Future<void> = createFuture()
    let dataFuture: Future<BlockBatch<Out> | undefined> = createFuture()
    let headFuture: Future<BlockRef | undefined> = createFuture()

    return {
        writer: new BlockWriter<In>({
            head: async () => head ?? headFuture.promise(),
            write: async (batch) => {
                await readyFuture.promise()
                const transformedBatch = await transform(batch)
                if (transformedBatch.blocks.length > 0) {
                    head = last(transformedBatch.blocks).header
                }
                dataFuture.resolve(transformedBatch)
                dataFuture = createFuture()
            },
            fork: async (refs) => {
                const forkError = new ForkException(refs)
                readyFuture.reject(forkError)
                dataFuture.reject(forkError)

                state = 'idle'
                head = undefined
                headFuture.resolve(undefined)
                headFuture = createFuture()
            },
            close: async () => {
                if (state === 'closed') return

                state = 'closed'
                readyFuture.reject(new Error('BlockTransformer closed'))
                dataFuture.resolve(undefined)
                headFuture.resolve(undefined)
            },
        }),
        reader: new BlockReader<Out>({
            head: async () => head,
            read: async function* (req) {
                if (state === 'closed') return

                if (state === 'reading') {
                    throw new Error('BlockTransformer is already read')
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

export interface BlockFinalizerOptions {
    finalityConfirmation?: number
    throwOnFork?: boolean
}

export function finalizer<B extends BlockBase>(
    opts: BlockFinalizerOptions = {},
): BlockWriterReaderPair<B, B> {
    let head: BlockRef | undefined
    let state: 'idle' | 'reading' | 'closed' = 'idle'

    let readyFuture: Future<void> = createFuture()
    let dataFuture: Future<BlockBatch<B> | undefined> = createFuture()
    let headFuture: Future<BlockRef | undefined> = createFuture()

    const buffer: BlockBatch<B>['blocks'] = []

    return {
        writer: new BlockWriter<B>({
            head: async () => head ?? headFuture.promise(),
            write: async (batch) => {
                await readyFuture.promise()

                buffer.push(...batch.blocks)
                const lastBlock = maybeLast(buffer)

                if (opts.finalityConfirmation || batch.finalizedHead) {
                    const finalizedNumber = opts.finalityConfirmation
                        ? (batch.head ?? last(buffer).header).number - opts.finalityConfirmation
                        : batch.finalizedHead!.number
                    const unfinalizedIndex = buffer.findIndex((block) => block.header.number > finalizedNumber)
                    if (unfinalizedIndex > 0) {
                        dataFuture.resolve({
                            blocks: buffer.splice(0, unfinalizedIndex),
                            finalizedHead: batch.finalizedHead,
                            head: batch.finalizedHead,
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
            head: async () => head,
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
    throw new Error('not implemented')
}

export function concat<B extends BlockBase>(...readers: BlockReader<B>[]): BlockReader<B> {
    throw new Error('not implemented')
}

export function fallback<B extends BlockBase>(...readers: BlockReader<B>[]): BlockReader<B> {
    throw new Error('not implemented')
}
