import {HttpClient} from '@sqd-sdk/core/http-client'
import {createLogger} from '@sqd-sdk/core/logger'
import {BlockWriter, transformer, type BlockRef, BlockReader, ForkException, finalizer} from '@sqd-sdk/core/pipeline'
import {PortalClient} from '@sqd-sdk/core/portal-client'
import {createSolanaPortalDataReader} from '@sqd-sdk/solana-stream'
import sqlite3 from 'better-sqlite3'

async function main() {
    let portal = new PortalClient({
        url: 'https://portal.sqd.dev/datasets/solana-mainnet',
        http: new HttpClient({
            retryAttempts: Number.POSITIVE_INFINITY,
        }),
        minBytes: 100 * 1024 * 1024,
    })

    let fromBlock = await portal.getHead().then((h) => (h?.number ?? 0) - 50_000)

    const db = new sqlite3('./test.db')

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
        .pipeThrough(createProgressTracker('portal'))
        .pipeThrough(finalizer({finalityConfirmation: 1000}))
        .pipeThrough({
            writer: createSqliteWriter(db),
            reader: createSqliteReader(db),
        })
        .pipeThrough(createProgressTracker('sqlite'))
        .pipeTo(
            new BlockWriter({
                head: async () => undefined,
                write: async () => {},
                close: async () => {},
                fork: async () => {},
            }),
        )
}

function createSqliteWriter(db: sqlite3.Database) {
    db.exec(`CREATE TABLE IF NOT EXISTS blocks (
            number INTEGER PRIMARY KEY,
            timestamp INTEGER,
            hash TEXT
        )`)

    const headStmt = db.prepare<[], BlockRef>('SELECT number, hash FROM blocks ORDER BY number DESC LIMIT 1')
    const headsStmt = db.prepare<[number], BlockRef>(
        'SELECT number, hash FROM blocks WHERE number >= ? ORDER BY number ASC',
    )
    const rollbackStmt = db.prepare<[number], void>('DELETE FROM blocks WHERE number >= ?')

    return new BlockWriter<{header: BlockRef & {timestamp: number}}>({
        head: async () => headStmt.get(),
        write: async ({blocks, finalizedHead}) => {
            if (blocks.length > 0) {
                const values = blocks
                    .map((block) => `(${block.header.number}, ${block.header.timestamp}, '${block.header.hash!}')`)
                    .join(', ')
                const sql = `INSERT INTO blocks (number, timestamp, hash) VALUES ${values}`
                db.exec(sql)
            }

            await new Promise((resolve) => setTimeout(resolve, 0))
        },
        fork: async (refs) => {
            db.transaction(() => {
                const blocks = headsStmt.all(refs[0].number)
                for (let i = 0; i < refs.length; i++) {
                    const ref = refs[i]
                    let block: BlockRef | undefined
                    while (blocks.length > 0) {
                        block = blocks[0]
                        if (block.number >= ref.number) break
                        blocks.shift()
                    }
                    if (block == null) break
                    if (block.number > ref.number) continue
                    if (block.number === ref.number && block.hash !== ref.hash) {
                        rollbackStmt.run(block.number)
                        break
                    }
                    blocks.shift()
                }
            })
        },
        close: async () => {},
    })
}

function createSqliteReader(db: sqlite3.Database) {
    const headStmt = db.prepare<[], BlockRef>('SELECT number, hash FROM blocks ORDER BY number DESC LIMIT 1')
    const headsStmt = db.prepare<[number], BlockRef & {timestamp: number}>(
        'SELECT * FROM blocks WHERE number >= ? ORDER BY number ASC LIMIT 10000',
    )
    const headsStmt2 = db.prepare<[number, number], BlockRef & {timestamp: number}>(
        'SELECT * FROM blocks WHERE number >= ? AND number <= ? ORDER BY number ASC LIMIT 10000',
    )
    const blockStmt = db.prepare<[number], BlockRef>('SELECT * FROM blocks WHERE number = ?')
    return new BlockReader<{header: BlockRef & {timestamp: number}}>({
        head: async () => headStmt.get(),
        read: async function* (req) {
            let head: BlockRef = {
                number: req.range?.from ?? 0,
                hash: req.parentHash,
            }

            while (true) {
                const blocks = db.transaction(() => {
                    const blocks =
                        req.range?.to == null ? headsStmt.all(head.number) : headsStmt2.all(head.number, req.range?.to)
                    if (head.hash && (blocks[0].number > head.number || blocks[0].hash !== head.hash)) {
                        const refs = db
                            .prepare<[number], BlockRef>('SELECT number, hash FROM blocks WHERE number <= ? LIMIT 100')
                            .all(head.number)
                        throw new ForkException(refs)
                    }

                    return blocks.slice(1)
                })()

                const lastBlock = blocks[blocks.length - 1]
                head = lastBlock
                    ? {
                          number: lastBlock.number,
                          hash: lastBlock.hash,
                      }
                    : head

                yield {
                    head,
                    blocks: blocks.map((block) => ({
                        header: block,
                    })),
                }

                await new Promise((resolve) => setTimeout(resolve, 0))
            }
        },
    })
}

function createProgressTracker(prefix: string) {
    const logger = createLogger(`sqd:${prefix}`)

    return transformer(async (batch) => {
        if (batch.blocks.length > 0) {
            const {blocks, finalizedHead, head} = batch
            let lastBlock = blocks[blocks.length - 1].header
            logger.info(
                [
                    `[${new Date().toISOString()}] progress: ${lastBlock.number} / ${Math.max(
                        head?.number ?? -1,
                        lastBlock.number,
                    )}`,
                    `blocks: ${blocks.length}`,
                    `finalized: ${finalizedHead?.number}`,
                ].join(', '),
            )
        }
        return batch
    })
}

main()
