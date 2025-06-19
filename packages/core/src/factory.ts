import type {BlockRef, DataSourceStreamOptions, BlockBatch, DataSource, DataSink} from './pipeline'

export interface DataSourceOptions<B> {
    getHead: () => Promise<BlockRef | undefined>
    getFinalizedHead: () => Promise<BlockRef | undefined>
    fetchData: (opts: DataSourceStreamOptions) => AsyncGenerator<BlockBatch<B>>
}

export interface DataSinkOptions<B> {
    getHead: () => Promise<BlockRef | undefined>
    getFinalizedHead: () => Promise<BlockRef | undefined>
    writeData: (block: B) => Promise<void>
    rollbackBlocks: (refs: BlockRef[]) => Promise<void>
    unfinalized?: boolean
}

export function createDataSource<B>(options: DataSourceOptions<B>): DataSource<B> {
    let stream: ReadableStream<BlockBatch<B>> | null = null

    return {
        get locked() {
            return stream?.locked ?? false
        },

        getHead: options.getHead,
        getFinalizedHead: options.getFinalizedHead,

        getStream(opts: DataSourceStreamOptions): ReadableStream<BlockBatch<B>> {
            if (this.locked) {
                throw new Error('Stream is already locked')
            }

            stream = new ReadableStream<BlockBatch<B>>({
                async pull(controller) {
                    try {
                        const generator = options.fetchData(opts)
                        for await (const batch of generator) {
                            controller.enqueue(batch)
                        }
                        controller.close()
                    } catch (error) {
                        controller.error(error)
                    }
                },
                cancel() {
                    stream = null
                }
            })

            return stream
        }
    }
}

export function createDataSink<B>(options: DataSinkOptions<B>): DataSink<B> {
    let stream: WritableStream<B> | null = null

    return {
        unfinalized: options.unfinalized,

        get locked() {
            return stream?.locked ?? false
        },

        getHead: options.getHead,
        getFinalizedHead: options.getFinalizedHead,
        rollbackBlocks: options.rollbackBlocks,

        getStream(): WritableStream<B> {
            if (this.locked) {
                throw new Error('Stream is already locked')
            }

            stream = new WritableStream<B>({
                write: async (block) => {
                    await options.writeData(block)
                },
                close() {
                    stream = null
                },
                abort() {
                    stream = null
                }
            })

            return stream
        }
    }
} 