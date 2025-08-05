import {HttpClient} from '@sqd-sdk/core/http-client'
import {createLogger} from '@sqd-sdk/core/logger'
import {
    type Data,
    type DataBatch,
    type UnfinalizedDataTarget,
    type DataRef,
    DataTarget,
    transformer,
    type UnfinalizedDataSource,
    type DataDuplexFactory,
} from '@sqd-sdk/core/pipeline'
import {PortalClient} from '@sqd-sdk/core/portal'
import {type SolanaPortalData, solanaPortalDataSource} from '@sqd-sdk/solana-stream'

async function main() {
    let portal = new PortalClient({
        url: 'https://portal.sqd.dev/datasets/solana-mainnet',
        http: new HttpClient({
            retryAttempts: Number.POSITIVE_INFINITY,
        }),
        minBytes: 100 * 1024 * 1024,
    })

    let fromBlock = await portal.getHead().then((h) => (h?.number ?? 0) - 50_000)

    const src = solanaPortalDataSource({
        portal,
        query: {
            fields: {
                block: {number: true, timestamp: true, hash: true, parentHash: true},
                transaction: {signatures: true, err: true, transactionIndex: true},
                instruction: {
                    programId: true,
                    accounts: true,
                    data: true,
                    isCommitted: true,
                    transactionIndex: true,
                    instructionAddress: true,
                },
            },
            requests: [
                {
                    range: {from: fromBlock},
                    request: {
                        instructions: [
                            {
                                programId: ['whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'],
                                d8: ['0xf8c69e91e17587c8'],
                                isCommitted: true,
                                innerInstructions: true,
                            },
                        ],
                    },
                },
            ],
        },
    })

    await src.pipeThrough(createProgressTracker('solana')).pipeTo(
        new DataTarget({
            unfinalized: true,
            writer: async () => {
                return {
                    offset: undefined,
                    write: async (batch) => {
                        return batch.offset
                    },
                    fork: async (fork) => {
                        return fork.heads[0]
                    },
                }
            },
        }),
    )
}

interface StateManager<T extends Data<any, any>> {
    get(): Promise<T['ref'] | undefined>
    set(ref: T['ref']): Promise<void>
    fork(refs: T['ref'][]): Promise<T['ref'] | undefined>
}

function createStateTarget<T extends Data<any, any>>(opts: {
    state: StateManager<T>
    transact: (batch: DataBatch<T>) => Promise<unknown>
    rollback: (block: DataRef<T>) => Promise<unknown>
}): UnfinalizedDataTarget<T> {
    const {state, transact, rollback} = opts

    return new DataTarget<T>({
        unfinalized: true,
        writer: async () => {
            const head = await state.get()
            if (head) {
                await rollback(head)
            }

            return {
                offset: head,
                write: async (batch) => {
                    await transact(batch)

                    if (batch.data.length > 0) {
                        await state.set(batch.data[batch.data.length - 1].ref)
                    }
                },
                fork: async (fork) => {
                    const newHead = await state.fork(fork.heads)
                    if (newHead) {
                        await rollback(newHead)
                    }
                    return newHead
                },
            }
        },
    })
}

function createProgressTracker<T extends Data<any, {number: number}>>(
    prefix: string,
): DataDuplexFactory<{
    target: UnfinalizedDataTarget<T>
    source: UnfinalizedDataSource<T>
}> {
    const logger = createLogger(`sqd:${prefix}`)

    return transformer<T, T>({
        transformer: async (opts) => {
            return {
                offset: opts.offset,
                transform: async (batch) => {
                    if (batch.data.length > 0) {
                        const {offset, head} = batch
                        logger.info(
                            [
                                `[${new Date().toISOString()}] progress: ${offset.number} / ${head.number}`,
                                `blocks: ${batch.data.length}`,
                            ].join(', '),
                        )
                    }
                    return batch
                },
                fork: async (fork) => fork,
            }
        },
    })
}

main()
