import {AsyncQueue} from './internal/async'
import {assert} from './internal/misc'
import type {Range} from './internal/range'

export interface BlockRef {
    number: number
    hash: string
}

export interface DataSourceStreamOptions {
    range?: Range
    parentHash?: string
}

export interface BlockBatch<B> {
    blocks: B[]
    finalizedHead?: BlockRef
    head?: BlockRef
}

export interface BlockReader<B> {
    stream(req: DataSourceStreamOptions): AsyncIterable<BlockBatch<B>>
    pipeTo(writer: BlockWriter<B>): Promise<void>
}

export class ForkException extends Error {
    readonly isSqdForkException = true

    constructor(
        expectedParentHash: string,
        nextBlock: number,
        public readonly previousBlocks: BlockRef[],
    ) {
        assert(previousBlocks.length > 0)
        let last = previousBlocks[previousBlocks.length - 1]
        super(
            `expected ${nextBlock} to have parent ${last.number}#${expectedParentHash}, but got ${last.number}#${last.hash}`,
        )
    }

    get name(): string {
        return 'ForkException'
    }
}

export function isForkException(err: unknown): err is ForkException {
    return err instanceof Error && !!(err as Partial<ForkException>).isSqdForkException
}

export class DataConsistencyError extends Error {
    readonly isSqdDataConsistencyError = true
}

export class BlockConsistencyError extends DataConsistencyError {
    constructor(ref: BlockRef, errorMsg?: string) {
        let msg = `Failed to fetch block ${ref.number}#${ref.hash}`
        if (errorMsg) {
            msg += `: ${errorMsg}`
        }
        super(msg)
    }
}

export function isDataConsistencyError(err: unknown): err is Error {
    return err instanceof Error && !!(err as Partial<DataConsistencyError>).isSqdDataConsistencyError
}

export interface BlockWriter<B> {
    head(): Promise<BlockRef | undefined>
    put(batch: BlockBatch<B>): Promise<void>
    fork?(refs: BlockRef[]): Promise<void>
}

export interface BlockSource<B> {
    head(): Promise<BlockRef | undefined>
    stream(req: DataSourceStreamOptions): AsyncIterable<BlockBatch<B>>
}

export interface BlockSink<B> {
    head(): Promise<BlockRef | undefined>
    put(batch: BlockBatch<B>): Promise<void>
    fork?(refs: BlockRef[]): Promise<void>
}

export class BlockReader<B = BlockRef> implements BlockReader<B> {
    constructor(private source: BlockSource<B>) {}

    stream(req: DataSourceStreamOptions): AsyncIterable<BlockBatch<B>> {
        return this.source.stream(req)
    }

    pipeTo(writer: BlockWriter<B>, opts?: DataSourceStreamOptions): Promise<void> {
        return BlockReaderPipeTo(this, writer, opts)
    }

    pipeThrough<T>(pair: BlockWriterReaderPair<B, T>, opts?: DataSourceStreamOptions): BlockReader<T> {
        BlockReaderPipeTo(this, pair.writer, opts).catch((err) => {
            throw err
        })

        return pair.reader
    }
}

export interface BlockWriterReaderPair<W, R> {
    writer: BlockWriter<W>
    reader: BlockReader<R>
}

export class BlockWriter<B> implements BlockWriter<B> {
    constructor(private sink: BlockSink<B>) {}

    head(): Promise<BlockRef | undefined> {
        return this.sink.head()
    }

    put(batch: BlockBatch<B>): Promise<void> {
        return this.sink.put(batch)
    }

    fork?(refs: BlockRef[]): Promise<void> {
        return this.sink.fork?.(refs) || Promise.resolve()
    }
}

export class BlockTransformer<B, T> implements BlockWriterReaderPair<B, T> {
    readonly writer: BlockWriter<B>
    readonly reader: BlockReader<T>

    private queue: AsyncQueue<BlockBatch<T>>

    constructor(private transform: (batch: BlockBatch<B>) => BlockBatch<T>) {
        this.queue = new AsyncQueue(1)

        this.writer = new BlockWriter<B>({
            head: async () => undefined,
            put: async (batch) => {
                await this.queue.put(this.transform(batch))
            },
        })

        this.reader = new BlockReader<T>({
            head: async () => undefined,
            stream: () => this.queue.iterate(),
        })
    }
}

async function BlockReaderPipeTo<B>(
    reader: BlockReader<B>,
    writer: BlockWriter<B>,
    opts?: DataSourceStreamOptions,
): Promise<void> {
    while (true) {
        let head = await writer.head()

        let {from, parentHash} = head
            ? opts?.range?.from
                ? head.number + 1 <= opts.range.from
                    ? {from: head.number + 1, parentHash: head.hash}
                    : {from: opts.range.from, parentHash: opts.parentHash}
                : {from: head.number + 1, parentHash: head.hash}
            : {from: 0, parentHash: undefined}

        try {
            for await (const batch of reader.stream({
                range: {from, to: opts?.range?.to},
                parentHash,
            })) {
                await writer.put(batch)
            }
        } catch (err) {
            if (isForkException(err) && writer.fork) {
                await writer.fork(err.previousBlocks)
            }
            throw err
        }
    }
}
