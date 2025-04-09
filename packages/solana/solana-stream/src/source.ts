import {applyRangeBound, mergeRangeRequests, type RangeRequest, type Range} from '@sqd-sdk/core/internal/range/index'
import type {DataSource, DataSourceStream, BlockBatch, DataSourceStreamOptions} from '@sqd-sdk/core/pipeline'
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
import {type DataRequest, mergeDataRequests, type SolanaQueryOptions} from './query'
import {PortalClient, type PortalStreamData, type PortalClientOptions} from '@sqd-sdk/core/portal-client'
import {type MergeSelection, mergeSelection} from '@sqd-sdk/core/internal/selection'

export interface SolanaPortalDataSourceOptions<Q extends SolanaQueryOptions> {
    portal: PortalClientOptions | PortalClient
    query: Q
}

export class SolanaPortalDataSource<
    Q extends SolanaQueryOptions,
    B extends Block<GetFields<Q['fields']>> = Block<GetFields<Q['fields']>>,
> implements DataSource<B>
{
    private portal: PortalClient
    private fields: Q['fields']
    private requests: RangeRequest<DataRequest>[]

    constructor(options: SolanaPortalDataSourceOptions<Q>) {
        this.portal = options.portal instanceof PortalClient ? options.portal : new PortalClient(options.portal)
        this.fields = options.query.fields
        this.requests = mergeRangeRequests(options.query.requests, mergeDataRequests)
    }

    getHead() {
        return this.portal.getHead()
    }

    getFinalizedHead() {
        return this.portal.getFinalizedHead()
    }

    getStream(opts?: DataSourceStreamOptions): DataSourceStream<B> {
        let fields = getFields(this.fields)
        let requests = applyRangeBound(this.requests, opts?.range)

        let {writable, readable} = new TransformStream<PortalStreamData<Block<typeof fields>>, BlockBatch<B>>({
            transform: async (data, controller) => {
                let blocks = data.blocks.map((b) => {
                    let block = mapBlock(b, fields)
                    return block
                }) as BlockBatch<B>['blocks']

                controller.enqueue({
                    blocks,
                    finalizedHead: data.finalizedHead,
                })
            },
        })

        const ingest = async () => {
            for (let request of requests) {
                let query = {
                    type: 'solana',
                    fromBlock: request.range.from,
                    toBlock: request.range.to,
                    fields,
                    ...request.request,
                }

                await this.portal.getFinalizedStream(query, {stopOnHead: opts?.stopOnHead}).pipeTo(writable, {
                    preventClose: true,
                })
            }
        }

        ingest()
            .then(
                () => writable.close(),
                (reason) => writable.abort(reason),
            )
            .catch(() => {})

        return readable
    }
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
