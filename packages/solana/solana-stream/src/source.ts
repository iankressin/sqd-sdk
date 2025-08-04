import {applyRangeBound, mergeRangeRequests} from '@sqd-sdk/core/internal/range/index'
import {
    type DataBatch,
    type DataRef,
    type UnfinalizedDataSource,
    ForkException,
    type Data,
    DataSource,
} from '@sqd-sdk/core/pipeline'
import {cast} from '@sqd-sdk/core/validation'
import {
    type Block,
    blockFromPartial,
    type BlockPartial,
    type FieldSelection,
    type RequiredFieldSelection,
    REQUIRED_FIELDS,
} from './objects'
import {getDataSchema} from './schema'
import {setUpRelations} from './objects/relations'
import {mergeDataRequests, type SolanaQueryOptions} from './query'
import {PortalClient, type PortalClientOptions, isForkException} from '@sqd-sdk/core/portal'
import {type MergeSelection, mergeSelection} from '@sqd-sdk/core/internal/selection'
import {assert, last} from '@sqd-sdk/core/internal/misc'
import {Throttler} from '@sqd-sdk/core/internal/throttler'

type GetFields<F extends FieldSelection> = MergeSelection<RequiredFieldSelection, F>

export interface SolanaPortalDataReaderOptions<Q extends SolanaQueryOptions> {
    portal: PortalClientOptions | PortalClient
    query: Q
}

export interface BlockRefValue {
    readonly number: number
    readonly hash?: string
}

export type SolanaPortalData<Q extends SolanaQueryOptions> = Data<Block<GetFields<Q['fields']>>, BlockRef>

export class BlockRef implements DataRef<BlockRef> {
    static fromBlock(block: {header: {number: number; hash?: string}}): BlockRef {
        return new BlockRef(block.header)
    }

    readonly number: number
    readonly hash?: string

    constructor(value: BlockRefValue) {
        this.number = value.number
        this.hash = value.hash
    }

    compare(other: BlockRef) {
        if (this.number < other.number) return 'ls'
        if (this.number > other.number) return 'gt'
        if (this.hash === other.hash) return 'eq'
        return 'fk'
    }
}

function calculateHead(portalHead: BlockRef, lastBlock: BlockRef | undefined): BlockRef {
    if (!lastBlock) return portalHead
    return lastBlock.compare(portalHead) === 'gt' ? lastBlock : portalHead
}

export function solanaPortalDataSource<Q extends SolanaQueryOptions>(
    options: SolanaPortalDataReaderOptions<Q>,
): UnfinalizedDataSource<SolanaPortalData<Q>> {
    const portal = options.portal instanceof PortalClient ? options.portal : new PortalClient(options.portal)
    const fields = getFields(options.query.fields)
    const requests = mergeRangeRequests(options.query.requests, mergeDataRequests)
    const headThrottler = new Throttler(async () => portal.getHead(), 5_000)

    const createDataStream = async function* (
        offset?: BlockRef,
    ): AsyncIterableIterator<DataBatch<SolanaPortalData<Q>>> {
        let parentHash = offset?.hash
        const requestsBounded = offset ? applyRangeBound(requests, {from: offset.number + 1}) : requests

        for (const request of requestsBounded) {
            let fromBlock = request.range.from

            while (request.range.to == null || fromBlock > request.range.to) {
                const query = {
                    type: 'solana' as const,
                    fromBlock,
                    parentHash,
                    toBlock: request.range.to,
                    fields,
                    ...request.request,
                }

                let dataProcessed = false

                try {
                    for await (const batch of portal.getStream(query)) {
                        const portalHead = await headThrottler.get()
                        if (!portalHead) continue // no data?

                        // FIXME: investigate type issue
                        const data = batch.blocks.map((b) => {
                            const value = mapBlock(b, fields)
                            return {
                                value,
                                ref: BlockRef.fromBlock(value),
                            }
                        }) as SolanaPortalData<Q>[]

                        const offset = last(data).ref
                        const head = calculateHead(new BlockRef(portalHead), offset)
                        const finalizedHead = batch.finalizedHead
                            ? calculateHead(new BlockRef(batch.finalizedHead), offset)
                            : undefined

                        yield {
                            data,
                            finalizedHead,
                            head,
                            offset,
                        }

                        if (offset) {
                            fromBlock = offset.number + 1
                            dataProcessed = true
                        }
                    }
                } catch (err) {
                    if (isForkException(err)) {
                        throw new ForkException<SolanaPortalData<Q>>({
                            heads: err.lastBlocks.map((b) => new BlockRef(b)),
                        })
                    }
                    throw err
                }

                if (!dataProcessed && query.fromBlock === fromBlock) {
                    assert(request.range.to != null)
                    fromBlock = request.range.to + 1
                }
            }
        }
    }

    return new DataSource<SolanaPortalData<Q>>({
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

export function mapBlock<F extends RequiredFieldSelection>(
    rawBlock: unknown,
    fields: RequiredFieldSelection,
): Block<F> {
    const validator = getDataSchema(fields)
    const partial = cast(validator, rawBlock) as BlockPartial<F>
    const block = blockFromPartial(partial)
    setUpRelations(block)

    return block
}

function getFields<T extends FieldSelection>(fields: T): GetFields<T> {
    return mergeSelection(REQUIRED_FIELDS, fields)
}
