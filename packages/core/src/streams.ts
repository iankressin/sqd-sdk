import type {BlockRef, DataSourceStreamOptions, BlockBatch, DataSource, DataSink} from './pipeline'

/**
 * Base implementation of a ReadableStream for block data
 */
export class BlockReadableStream<B> extends ReadableStream<BlockBatch<B>> {
    private controller: ReadableStreamDefaultController<BlockBatch<B>> | null = null
    private isClosed = false

    constructor(
        private readonly fetchData: (opts: DataSourceStreamOptions) => AsyncGenerator<BlockBatch<B>>,
        private readonly options: DataSourceStreamOptions
    ) {
        super({
            start: (controller) => {
                this.controller = controller
            },
            pull: async (controller) => {
                try {
                    const generator = this.fetchData(this.options)
                    for await (const batch of generator) {
                        if (this.isClosed) break
                        controller.enqueue(batch)
                    }
                    if (!this.isClosed) {
                        controller.close()
                    }
                } catch (error) {
                    controller.error(error)
                }
            },
            cancel: () => {
                this.isClosed = true
            }
        })
    }

    get locked(): boolean {
        return super.locked
    }
}

/**
 * Base implementation of a WritableStream for block data
 */
export class BlockWritableStream<B> extends WritableStream<B> {
    private controller: WritableStreamDefaultController | null = null
    private isClosed = false

    constructor(
        private readonly writeData: (block: B) => Promise<void>
    ) {
        super({
            start: (controller) => {
                this.controller = controller
            },
            write: async (block) => {
                try {
                    await this.writeData(block)
                } catch (error) {
                    this.controller?.error(error)
                }
            },
            close: () => {
                this.isClosed = true
            },
            abort: (reason) => {
                this.isClosed = true
            }
        })
    }

    get locked(): boolean {
        return super.locked
    }
} 