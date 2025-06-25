import {applyRangeBound, getRangeAt, getRequestAt, mergeRangeRequests} from '@sqd-sdk/core/internal/range/index'
import {BlockReader, type BlockBatch} from '@sqd-sdk/core/pipeline'
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

export interface SolanaPortalDataReaderOptions<Q extends SolanaQueryOptions> {
    portal: PortalClientOptions | PortalClient
    query: Q
}

export function createSolanaPortalDataReader<Q extends SolanaQueryOptions>(
    options: SolanaPortalDataReaderOptions<Q>,
): BlockReader<Block<GetFields<Q['fields']>>> {
    const portal = options.portal instanceof PortalClient ? options.portal : new PortalClient(options.portal)
    const fields = getFields(options.query.fields)
    const requests = mergeRangeRequests(options.query.requests, mergeDataRequests)

    return new BlockReader({
        head: async () => portal.getHead(),
        stream: async function* (opts) {
            let requestsBounded = applyRangeBound(requests, opts?.range)

            let fromBlock = requestsBounded[0]?.range.from ?? 0
            while (true) {
                let request = getRangeAt(requestsBounded, fromBlock)
                if (request == null) break

                let query = {
                    type: 'solana',
                    fromBlock,
                    toBlock: request.range.to,
                    fields,
                    ...request.request,
                }

                for await (let batch of portal.getStream(query)) {
                    yield {
                        blocks: batch.blocks.map((b) => mapBlock(b, fields)) as BlockBatch<
                            Block<GetFields<Q['fields']>>
                        >['blocks'],
                        finalizedHead: batch.finalizedHead,
                    }

                    const lastBlock = maybeLast(batch.blocks)
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
        },
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
