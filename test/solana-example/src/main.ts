import {HttpClient} from '@sqd-sdk/core/http-client'
import {createFuture} from '@sqd-sdk/core/internal/async'
import {last} from '@sqd-sdk/core/internal/misc'
import {
    BlockReader,
    BlockWriter,
    type DataSourceStreamOptions,
    type BlockWriterReaderPair,
    type BlockRef,
    ForkException,
} from '@sqd-sdk/core/pipeline'
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
                log: {
                    programId: true,
                    kind: true,
                    message: true,
                    instructionAddress: true,
                    logIndex: true,
                    transactionIndex: true,
                },
                balance: {
                    pre: true,
                    post: true,
                    transactionIndex: true,
                    account: true,
                },
                tokenBalance: {
                    preMint: true,
                    preDecimals: true,
                    preOwner: true,
                    preAmount: true,
                    postMint: true,
                    postDecimals: true,
                    postOwner: true,
                    postAmount: true,
                    transactionIndex: true,
                    account: true,
                },
                reward: {rewardType: true, pubkey: true},
            },
            requests: [
                {
                    range: {from: fromBlock, to: fromBlock + 100_000},
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
        .pipeThrough(createIntermediate())
        .pipeThrough(createIntermediate())
        .pipeThrough(createIntermediate())
        .pipeTo(
            new BlockWriter({
                head: async () => undefined,
                put: async ({blocks, finalizedHead}) => {
                    let lastBlock = blocks[blocks.length - 1].header as any
                    console.log(
                        [
                            `[${new Date().toISOString()}] progress: ${lastBlock.number} / ${Math.max(
                                finalizedHead?.number ?? -1,
                                lastBlock.number,
                            )}`,
                            `blocks: ${blocks.length}`,
                            `lag: ${(Date.now() - lastBlock.timestamp * 1000) / 1000}`,
                        ].join(', '),
                    )
                },
                fork: async (refs) => {
                    console.warn(
                        `[${new Date().toISOString()}] fork: ${refs.map((r) => `${r.number}#${r.hash.slice(0, 8)}`).join(', ')}`,
                    )
                },
            }),
        )
}

function createIntermediate<B extends {header: BlockRef}>(): BlockWriterReaderPair<B, B> {
    const startPromise = createFuture<DataSourceStreamOptions>()
    const blocks: B[] = []
    let finalizedHead: BlockRef | undefined = undefined

    let isForked = false

    return {
        writer: new BlockWriter({
            head: async () => undefined,
            put: async (batch) => {
                blocks.push(...batch.blocks)
                finalizedHead = batch.finalizedHead
            },
            fork: async (refs) => {
                for (const ref of refs.reverse()) {
                    const lastBlock = last(blocks)
                    if (lastBlock?.header.number === ref.number && lastBlock.header.hash === ref.hash) {
                        blocks.pop()
                        isForked = true
                    }
                }
            },
        }),
        reader: new BlockReader({
            head: async () => undefined,
            stream: async function* (opts) {
                const startBlock = opts.range ? blocks.find((b) => b.header.number === opts.range!.from) : blocks[0]
                if (opts.parentHash && startBlock?.header.hash !== opts.parentHash) {
                    throw new ForkException(opts.parentHash, {} as any, {} as any)
                }

                for (const block of blocks.slice()) {
                    yield {blocks: [block], finalizedHead: startBlock?.header}
                    if (isForked) break
                }
            },
        }),
    }
}

main()
