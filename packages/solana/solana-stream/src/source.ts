import {applyRangeBound, mergeRangeRequests} from '@sqd-sdk/core/internal/range/index'
import {type DataBatch, type DataRef, type DataRefer, source, type UnfinalizedDataSource} from '@sqd-sdk/core/pipeline'
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
import {PortalClient, type PortalClientOptions} from '@sqd-sdk/core/portal-client'
import {type MergeSelection, mergeSelection} from '@sqd-sdk/core/internal/selection'
import {assert, last} from '@sqd-sdk/core/internal/misc'
import {Throttler} from '@sqd-sdk/core/internal/throttler'

type GetFields<F extends FieldSelection> = MergeSelection<RequiredFieldSelection, F>

export interface SolanaPortalDataReaderOptions<Q extends SolanaQueryOptions> {
    portal: PortalClientOptions | PortalClient
    query: Q
}

export interface BlockRef {
    number: number
    hash?: string
}

export interface SolanaPortalData<Q extends SolanaQueryOptions> {
    item: Block<GetFields<Q['fields']>>
    ref: BlockRef
}

export const blockRefer: DataRefer<SolanaPortalData<any>> = {
    get(block: {header: {number: number; hash?: string}}): BlockRef {
        return {number: block.header.number, hash: block.header.hash}
    },

    compare(a: BlockRef, b: BlockRef) {
        if (a.number < b.number) return 'ls'
        if (a.number > b.number) return 'gt'
        if (a.hash === b.hash) return 'eq'
        return 'fk'
    },
}

function calculateHead(portalHead: BlockRef, lastBlock: BlockRef | undefined): BlockRef {
    if (!lastBlock) return portalHead
    return blockRefer.compare(lastBlock, portalHead) === 'gt' ? lastBlock : portalHead
}

export function solanaPortalDataSource<Q extends SolanaQueryOptions>(
    options: SolanaPortalDataReaderOptions<Q>
): UnfinalizedDataSource<SolanaPortalData<Q>> {
    const portal = options.portal instanceof PortalClient ? options.portal : new PortalClient(options.portal)
    const fields = getFields(options.query.fields)
    const requests = mergeRangeRequests(options.query.requests, mergeDataRequests)
    const headThrottler = new Throttler(async () => portal.getHead(), 5_000)

    const createDataStream = async function* (
        offset?: DataRef<SolanaPortalData<Q>>
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
                for await (const batch of portal.getStream(query)) {
                    const portalHead = await headThrottler.get()
                    if (!portalHead) continue // no data?

                    const lastRef = batch.blocks.length > 0 ? blockRefer.get(last(batch.blocks)) : undefined
                    const head = calculateHead(portalHead, lastRef)
                    const next = lastRef

                    yield {
                        data: batch.blocks.map((b) => mapBlock(b, fields)),
                        finalizedHead: batch.finalizedHead,
                        head: next && blockRefer.compare(next, head) === 'gt' ? next : head,
                        next,
                    }

                    if (lastRef) {
                        fromBlock = lastRef.number + 1
                        dataProcessed = true
                    }
                }

                if (!dataProcessed && query.fromBlock === fromBlock) {
                    assert(request.range.to != null)
                    fromBlock = request.range.to + 1
                }
            }
        }
    }

    return source<SolanaPortalData<Q>>({
        unfinalized: true,
        reader: async (offset) => {
            const stream = createDataStream(offset)

            return {
                read: async () => {
                    const batch = await stream.next()
                    if (batch.done) {
                        throw new Error('No more data')
                    }
                    return batch.value
                },
                close: async () => stream.return?.(),
            }
        },
        ref: blockRefer,
    })
}

export function mapBlock<F extends RequiredFieldSelection>(
    rawBlock: unknown,
    fields: RequiredFieldSelection
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
