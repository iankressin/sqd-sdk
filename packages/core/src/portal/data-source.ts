import {assert, last} from '../internal/misc'
import {Throttler} from '../internal/throttler'
import {
    type Data,
    type DataBatch,
    type DataRef,
    DataSource,
    ForkException,
    type UnfinalizedDataSource,
} from '../pipeline'
import {
    isForkException,
    PortalClient,
    type PortalQuery,
    type BlockRef as BlockRef_,
    type PortalClientOptions,
} from './client'

export interface PortalDataSourceOptions {
    portal: PortalClientOptions | PortalClient
    query: PortalQuery
}

export class BlockId implements DataRef<BlockRef_> {
    static fromBlock(block: {header: BlockRef_}): BlockId {
        return new BlockId(block.header)
    }

    readonly number: number
    readonly hash?: string

    constructor(value: BlockRef_) {
        this.number = value.number
        this.hash = value.hash
    }

    compare(other: BlockRef_) {
        if (this.number < other.number) return 'ls'
        if (this.number > other.number) return 'gt'
        if (this.hash === other.hash) return 'eq'
        return 'fk'
    }
}

function calculateHead(portalHead: BlockId, lastBlock: BlockId | undefined): BlockId {
    if (!lastBlock) return portalHead
    return lastBlock.compare(portalHead) === 'gt' ? lastBlock : portalHead
}

export function portalDataSource<T extends Data>(options: PortalDataSourceOptions): UnfinalizedDataSource<T> {
    const portal = options.portal instanceof PortalClient ? options.portal : new PortalClient(options.portal)
    const headThrottler = new Throttler(async () => portal.getHead(), 5_000)

    const createDataStream = async function* (offset?: BlockId): AsyncIterableIterator<DataBatch<T>> {
        let parentBlockHash: string | undefined
        let fromBlock = options.query.fromBlock ?? 0
        if (offset) {
            fromBlock = Math.max(offset.number + 1, fromBlock)
            parentBlockHash = fromBlock === offset.number + 1 ? offset.hash : undefined
        }
        const toBlock = options.query.toBlock

        while (toBlock == null || fromBlock <= toBlock) {
            const streamQuery = {
                ...options.query,
                fromBlock,
                parentBlockHash,
                toBlock,
            }

            try {
                for await (const batch of portal.getStream(streamQuery)) {
                    const portalHead = await headThrottler.get()
                    if (!portalHead) continue // no data?

                    // FIXME: investigate type issue
                    const data = batch.blocks.map((value) => {
                        return {
                            value,
                            ref: BlockId.fromBlock(value),
                        }
                    }) as T[]

                    const offset = last(data).ref
                    const head = calculateHead(new BlockId(portalHead), offset)
                    const finalizedHead = batch.finalizedHead
                        ? calculateHead(new BlockId(batch.finalizedHead), offset)
                        : undefined

                    yield {
                        data,
                        finalizedHead,
                        head,
                        offset,
                    }

                    fromBlock = offset.number + 1
                    parentBlockHash = offset.hash
                }
            } catch (err) {
                if (isForkException(err)) {
                    throw new ForkException<T>({
                        heads: err.lastBlocks.map((b) => new BlockId(b)),
                    })
                }
                throw err
            }
        }
    }

    return new DataSource<T>({
        unfinalized: true,
        reader: async (opts) => {
            const stream = createDataStream(opts.offset)

            return {
                read: async () => {
                    const batch = await stream.next()
                    return batch.done ? undefined : batch.value
                },
                close: async () => stream.return?.(),
            }
        },
    })
}
