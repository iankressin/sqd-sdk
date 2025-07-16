import {createClient} from '@clickhouse/client'
import {HttpClient} from '@sqd-sdk/core/http-client'
import {createLogger} from '@sqd-sdk/core/logger'
import {DataWriter, transformer, type Data, type DataBatch, type DataWriterReaderPair} from '@sqd-sdk/core/pipeline'
import {PortalClient} from '@sqd-sdk/core/portal-client'
import {createSolanaPortalDataReader} from '@sqd-sdk/solana-stream'

async function main() {
    let portal = new PortalClient({
        url: 'https://portal.sqd.dev/datasets/solana-mainnet',
        http: new HttpClient({
            retryAttempts: Number.POSITIVE_INFINITY,
        }),
        minBytes: 100 * 1024 * 1024,
    })

    let fromBlock = await portal.getHead().then((h) => (h?.number ?? 0) - 50_000)

    const client = createClient({
        url: 'http://localhost:8123',
    })

    await createSolanaPortalDataReader({
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
        .pipeThrough(createProgressTracker('progress'))
        .pipeTo(
            createStateWriter({
                state: {} as any,
                transact: async (batch) => {
                    await client.insert({
                        table: 'transactions',
                        values: batch.data.flatMap((block) =>
                            block.transactions.map((tx: any) => ({
                                number: block.header.number,
                                txHash: tx.signatures[0],
                            }))
                        ),
                    })
                },
                rollback: async (block) => {
                    await client.query({
                        query: `DELETE FROM transactions WHERE number > ${block.number}`,
                        format: 'JSONEachRow',
                    })
                },
            })
        )
}

interface StateManager<T extends Data<any, any>> {
    get(): Promise<T['ref'] | undefined>
    set(ref: T['ref']): Promise<void>
    fork(refs: T['ref'][]): Promise<T['ref'] | undefined>
}

function createStateWriter<T extends Data<any, any>>(opts: {
    state: StateManager<T>
    transact: (batch: DataBatch<T>) => Promise<unknown>
    rollback: (block: T['ref']) => Promise<unknown>
}): DataWriter<T> {
    const {state, transact, rollback} = opts

    return new DataWriter<T>({
        init: async () => {
            const head = await state.get()
            if (head) {
                await rollback(head)
            }

            return head
        },
        write: async (batch) => {
            await transact(batch)
            await state.set(batch.last)

            return batch.last
        },
        fork: async (refs) => {
            const newHead = await state.fork(refs)

            if (newHead) {
                await rollback(newHead)
            }

            return newHead
        },
    })
}

function createProgressTracker<T extends Data<any, {number: number; hash?: string}>>(
    prefix: string
): DataWriterReaderPair<T, T> {
    const logger = createLogger(`sqd:${prefix}`)

    return transformer({
        transform: async (batch) => {
            if (batch.data.length > 0) {
                const {data, finalizedHead, head, meta} = batch
                logger.info(
                    meta,
                    [
                        `[${new Date().toISOString()}] progress: ${batch.last?.number ?? 0} / ${head.number ?? 0}`,
                        `blocks: ${data.length}`,
                    ].join(', ')
                )
            }
            return batch
        },
    })
}

new DataWriter({
    write: async (batch: DataBatch<Data<number, number>>) => {
        console.log(batch)
    },
    fork: async (refs) => {
        console.log(refs)
        return 1
    },
}).finalized

main()
