import {applyRangeBound, mergeRangeRequests} from '@sqd-sdk/core/internal/range/index'
import {type DataBatch, DataReader, type DataRef, type DataRefer, type DataSource, source} from '@sqd-sdk/core/pipeline'
import {cast} from '@sqd-sdk/core/validation'
import {
    type Block,
    blockFromPartial,
    type BlockPartial,
    type FieldSelection,
    type ReqiredFieldSelection,
    REQUIRED_FIELDS,
} from './objects'
import {getDataSchema} from './schema'
import {setUpRelations} from './objects/relations'
import {mergeDataRequests, type SolanaQueryOptions} from './query'
import {PortalClient, type PortalClientOptions} from '@sqd-sdk/core/portal-client'
import {type MergeSelection, mergeSelection} from '@sqd-sdk/core/internal/selection'
import {assert, maybeLast} from '@sqd-sdk/core/internal/misc'
import {Throttler} from '@sqd-sdk/core/internal/throttler'

export interface SolanaPortalDataReaderOptions<Q extends SolanaQueryOptions> {
    portal: PortalClientOptions | PortalClient
    query: Q
}

export interface BlockRef {
    number: number
    hash?: string
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

export interface SolanaPortalData<Q extends SolanaQueryOptions> {
    item: Block<GetFields<Q['fields']>>
    ref: BlockRef
}

export function solanaPortalDataSource<Q extends SolanaQueryOptions>(
    options: SolanaPortalDataReaderOptions<Q>
): DataSource<SolanaPortalData<Q>> {
    const portal = options.portal instanceof PortalClient ? options.portal : new PortalClient(options.portal)
    const fields = getFields(options.query.fields)
    const requests = mergeRangeRequests(options.query.requests, mergeDataRequests)

    const headThrottler = new Throttler(async () => portal.getHead(), 5_000)

    const create = async function* (
        offset?: DataRef<SolanaPortalData<Q>>
    ): AsyncIterableIterator<DataBatch<SolanaPortalData<Q>>> {
        let {number, hash} = offset ?? {}
        let requestsBounded = applyRangeBound(requests, {
            from: number != null ? number + 1 : 0,
        })

        for (let request of requestsBounded) {
            let fromBlock = request.range.from
            let parentHash = hash
            while (true) {
                if (request.range.to != null && fromBlock > request.range.to) break

                let query = {
                    type: 'solana',
                    fromBlock,
                    parentHash,
                    toBlock: request.range.to,
                    fields,
                    ...request.request,
                }

                for await (let batch of portal.getStream(query)) {
                    const portalHead = await headThrottler.get()
                    const lastBlock = maybeLast(batch.blocks)

                    const head: BlockRef =
                        portalHead != null
                            ? lastBlock != null
                                ? lastBlock.number >= portalHead.number
                                    ? blockRefer.get(lastBlock)
                                    : portalHead
                                : portalHead
                            : {number: 0}

                    yield {
                        data: batch.blocks.map((b) => mapBlock(b, fields)),
                        finalizedHead: batch.finalizedHead,
                        head,
                    }

                    if (lastBlock != null) {
                        fromBlock = lastBlock.header.number + 1
                    }
                }

                if (query.fromBlock === fromBlock) {
                    // FIXME: wierd, but lets put it here for now
                    assert(request.range.to != null)
                    fromBlock = request.range.to + 1
                }
            }
        }
    }

    return source<SolanaPortalData<Q>>({
        reader: async (offset) => DataReader.fromAsync(create(offset)),
        ref: blockRefer,
    })
}

export function mapBlock<F extends ReqiredFieldSelection>(rawBlock: unknown, fields: ReqiredFieldSelection): Block<F> {
    let validator = getDataSchema(fields)
    // FIXME: cast return type is broken?
    let partial = cast(validator, rawBlock) as BlockPartial<F>
    let block = blockFromPartial(partial)
    setUpRelations(block)

    return block as unknown as Block<F>
}

function getFields<T extends FieldSelection>(fields: T): GetFields<T> {
    return mergeSelection(REQUIRED_FIELDS, fields)
}

type GetFields<F extends FieldSelection> = MergeSelection<ReqiredFieldSelection, F>
