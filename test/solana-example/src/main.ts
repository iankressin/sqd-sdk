import {HttpClient} from '@sqd-sdk/core/http-client'
import {type Data, type DataBatch, target, type UnfinalizedDataTarget, type DataRef} from '@sqd-sdk/core/pipeline'
import {PortalClient} from '@sqd-sdk/core/portal'
import {solanaPortalDataSource} from '@sqd-sdk/solana-stream'

async function main() {
    let portal = new PortalClient({
        url: 'https://portal.sqd.dev/datasets/solana-mainnet',
        http: new HttpClient({
            retryAttempts: Number.POSITIVE_INFINITY,
        }),
        minBytes: 100 * 1024 * 1024,
    })

    let fromBlock = await portal.getHead().then((h) => (h?.number ?? 0) - 50_000)

    await solanaPortalDataSource({
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
    }).pipeTo(
        //createStateWriter({
        //    state: {} as any,
        //    transact: async (batch) => {
        //        await client.insert({
        //            table: 'transactions',
        //            values: batch.data.flatMap((block) =>
        //                block.transactions.map((tx: any) => ({
        //                    number: block.header.number,
        //                    txHash: tx.signatures[0],
        //                }))
        //            ),
        //        })
        //    },
        //    rollback: async (block) => {
        //        await client.query({
        //            query: `DELETE FROM transactions WHERE number > ${block.number}`,
        //            format: 'JSONEachRow',
        //        })
        //    },
        //})
        target({
            unfinalized: true,
            writer: async () => {
                return {
                    offset: undefined,
                    write: async (batch) => {
                        console.log(`${batch.offset.number}/${batch.head.number}`)
                    },
                    fork: async (fork) => {
                        console.log(fork.heads[fork.heads.length - 1]?.number)
                        return fork.heads[0]
                    },
                }
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
    rollback: (block: DataRef<T>) => Promise<unknown>
}): UnfinalizedDataTarget<T> {
    const {state, transact, rollback} = opts

    return target<T>({
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

// function createProgressTracker<T extends SolanaPortalData<any>>(prefix: string) {
//     const logger = createLogger(`sqd:${prefix}`)
//
//     return transformer<T, T>({
//         transform: async (batch) => {
//             if (batch.data.length > 0) {
//                 const {data, head} = batch
//                 logger.info(
//                     [
//                         `[${new Date().toISOString()}] progress: ${last(data)?.header.number ?? 0} / ${
//                             head.number ?? 0
//                         }`,
//                         `blocks: ${data.length}`,
//                     ].join(', ')
//                 )
//             }
//             return batch
//         },
//         fork: async (fork) => {
//             return fork
//         },
//         cursor: blockRefer as DataCursor<T>,
//     })
// }

main()
