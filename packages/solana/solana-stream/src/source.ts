import {applyRangeBound, mergeRangeRequests} from '@sqd-sdk/core/internal/range'
import {type DataBatch, type DataRef, type DataCursor, source, type UnfinalizedDataSource} from '@sqd-sdk/core/pipeline'
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
import {PortalClient, type PortalClientOptions} from '@sqd-sdk/core/portal'
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

export const blockRefer = {
    get(block: {header: {number: number; hash?: string}}): BlockRef {
        return {number: block.header.number, hash: block.header.hash}
    },

    compare(a: BlockRef, b: BlockRef) {
        if (a.number < b.number) return 'ls'
        if (a.number > b.number) return 'gt'
        if (a.hash === b.hash) return 'eq'
        return 'fk'
    },
} satisfies DataCursor<SolanaPortalData<any>>

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

                    // FIXME: investigate type issue
                    const data = batch.blocks.map((b) => mapBlock(b, fields)) as Block<GetFields<Q['fields']>>[]

                    const lastRef = blockRefer.get(last(data))
                    const head = calculateHead(portalHead, lastRef)

                    yield {
                        data,
                        finalizedHead: batch.finalizedHead,
                        head,
                        offset: lastRef,
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
        cursor: blockRefer,
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
