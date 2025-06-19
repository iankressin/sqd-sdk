import type {BlockRef, DataSourceStreamOptions, BlockBatch, DataSource, DataSink} from './pipeline'
import {BlockReadableStream, BlockWritableStream} from './streams'

/**
 * Base implementation of DataSource that provides common functionality
 * and simplifies implementation of new data sources.
 */
export abstract class BaseDataSource<B> implements DataSource<B> {
    private stream: BlockReadableStream<B> | null = null

    get locked(): boolean {
        return this.stream?.locked ?? false
    }

    abstract getHead(): Promise<BlockRef | undefined>
    abstract getFinalizedHead(): Promise<BlockRef | undefined>

    /**
     * Implement this method to provide the actual data fetching logic
     */
    protected abstract fetchData(opts: DataSourceStreamOptions): AsyncGenerator<BlockBatch<B>>

    getStream(opts: DataSourceStreamOptions): ReadableStream<BlockBatch<B>> {
        if (this.locked) {
            throw new Error('Stream is already locked')
        }

        this.stream = new BlockReadableStream(this.fetchData.bind(this), opts)
        return this.stream
    }
}

/**
 * Base implementation of DataSink that provides common functionality
 * and simplifies implementation of new data sinks.
 */
export abstract class BaseDataSink<B> implements DataSink<B> {
    private stream: BlockWritableStream<B> | null = null
    unfinalized?: boolean

    get locked(): boolean {
        return this.stream?.locked ?? false
    }

    abstract getHead(): Promise<BlockRef | undefined>
    abstract getFinalizedHead(): Promise<BlockRef | undefined>
    abstract rollbackBlocks(refs: BlockRef[]): Promise<void>

    /**
     * Implement this method to provide the actual data writing logic
     */
    protected abstract writeData(block: B): Promise<void>

    getStream(): WritableStream<B> {
        if (this.locked) {
            throw new Error('Stream is already locked')
        }

        this.stream = new BlockWritableStream(this.writeData.bind(this))
        return this.stream
    }
} 