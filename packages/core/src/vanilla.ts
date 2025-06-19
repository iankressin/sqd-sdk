import type {BlockRef, DataSourceStreamOptions, BlockBatch, DataSource as IDataSource, DataSink as IDataSink} from './pipeline'

export interface DataSourceOptions<B> {
    getHead: () => Promise<BlockRef | undefined>
    getFinalizedHead: () => Promise<BlockRef | undefined>
    readData: (opts: DataSourceStreamOptions) => AsyncGenerator<BlockBatch<B>>
}

export interface DataSinkOptions<B> {
    getHead: () => Promise<BlockRef | undefined>
    getFinalizedHead: () => Promise<BlockRef | undefined>
    writeData: (batch: BlockBatch<B>) => Promise<void>
    rollbackBlocks: (refs: BlockRef[]) => Promise<void>
    unfinalized?: boolean
}

export class DataSource<B> implements IDataSource<B> {
    private stream: ReadableStream<BlockBatch<B>> | null = null

    constructor(private readonly options: DataSourceOptions<B>) {}

    get locked(): boolean {
        return this.stream?.locked ?? false
    }

    getHead(): Promise<BlockRef | undefined> {
        return this.options.getHead()
    }

    getFinalizedHead(): Promise<BlockRef | undefined> {
        return this.options.getFinalizedHead()
    }

    getStream(opts: DataSourceStreamOptions): ReadableStream<BlockBatch<B>> {
        if (this.locked) {
            throw new Error('Stream is already locked')
        }

        const self = this
        const stream = new ReadableStream<BlockBatch<B>>({
            async start(controller) {
                try {
                    const generator = self.options.readData(opts)
                    for await (const batch of generator) {
                        controller.enqueue(batch)
                    }
                    controller.close()
                } catch (error) {
                    controller.error(error)
                }
            },
            cancel: () => {
                self.stream = null
            }
        })
        this.stream = stream
        return stream
    }
}

export class DataSink<B> implements IDataSink<B> {
    private stream: WritableStream<B> | null = null
    private currentBatch: BlockBatch<B> = {blocks: []}

    constructor(private readonly options: DataSinkOptions<B>) {}

    get locked(): boolean {
        return this.stream?.locked ?? false
    }

    getHead(): Promise<BlockRef | undefined> {
        return this.options.getHead()
    }

    getFinalizedHead(): Promise<BlockRef | undefined> {
        return this.options.getFinalizedHead()
    }

    rollbackBlocks(refs: BlockRef[]): Promise<void> {
        return this.options.rollbackBlocks(refs)
    }

    getStream(): WritableStream<B> {
        if (this.locked) {
            throw new Error('Stream is already locked')
        }

        const self = this
        this.stream = new WritableStream<B>({
            write: async (block) => {
                self.currentBatch.blocks.push(block)
                if (self.currentBatch.blocks.length >= 100) { // Batch size of 100
                    await self.flush()
                }
            },
            close: async () => {
                await self.flush()
                self.stream = null
            },
            abort: () => {
                self.currentBatch.blocks = []
                self.stream = null
            }
        })

        return this.stream
    }

    private async flush(): Promise<void> {
        if (this.currentBatch.blocks.length > 0) {
            const batch = this.currentBatch
            this.currentBatch = {blocks: []}
            await this.options.writeData(batch)
        }
    }
} 