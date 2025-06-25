import {PortalClient} from './client'
import type * as Solana from './query/solana'
import {last} from '../internal/misc'
import {HttpClient} from '../http-client/client'

async function main() {
    let portal = new PortalClient({
        url: 'https://portal.sqd.dev/datasets/solana-beta',
        http: new HttpClient({
            retryAttempts: Number.POSITIVE_INFINITY,
        }),
        minBytes: 100 * 1024 * 1024,
    })

    let fromBlock = 341289582

    let query = {
        type: 'solana',
        fields: {
            block: {number: true, timestamp: true, hash: true, parentHash: true},
            transaction: {signatures: true, err: true, transactionIndex: true},
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
        },
        instructions: [
            {
                programId: ['whirLbMiicVdio4qvUfM5KAg6Ct8VwpYzGff3uctyCc'],
                d8: ['0xf8c69e91e17587c8'],
                isCommitted: true,
                innerInstructions: true,
                transactionTokenBalances: true,
            },
        ],
        fromBlock,
        toBlock: fromBlock,
    } as const satisfies Solana.FinalizedQuery

    for await (let {blocks, finalizedHead} of portal.getStream(query, {})) {
        console.log(blocks[0])
    }
    console.log('end')
}

main()
