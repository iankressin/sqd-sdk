import {PortalClient} from './client'
import type * as Solana from './query/solana'
import {last} from '../util'
import {HttpClient} from '../http-client/client'

async function main() {
    let portal = new PortalClient({
        url: 'https://portal.sqd.dev/datasets/solana-beta',
        http: new HttpClient({
            retryAttempts: Number.POSITIVE_INFINITY,
        }),
        minBytes: 100 * 1024 * 1024,
    })

    let fromBlock = await portal.getHead().then((h) => (h?.number ?? 0) - 50_000)

    let query = {
        type: 'solana',
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
            balance: {pre: true, post: true, transactionIndex: true, account: true},
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
        instructions: [
            {
                programId: ['whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'],
                d8: ['0xf8c69e91e17587c8'],
                isCommitted: true,
                innerInstructions: true,
            },
        ],
        fromBlock,
    } as const satisfies Solana.FinalizedQuery

    for await (let {blocks, finalizedHead} of portal.getStream(query, {stopOnHead: false})) {
        let lastBlock = last(blocks).header as any
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
    }
    console.log('end')
}

main()
