import {HttpClient} from '@sqd-sdk/core/http-client'
import {last} from '@sqd-sdk/core/internal/misc'
import {createLogger} from '@sqd-sdk/core/logger'
import {DataWriter, transformer, ForkException, finalizer, DataReader, DataRef} from '@sqd-sdk/core/pipeline'
import {PortalClient} from '@sqd-sdk/core/portal-client'
import {BlockRef, createSolanaPortalDataReader, type SolanaPortalData} from '@sqd-sdk/solana-stream'
import Sqlite3 from 'better-sqlite3'

async function main() {
    let portal = new PortalClient({
        url: 'https://portal.sqd.dev/datasets/solana-mainnet',
        http: new HttpClient({
            retryAttempts: Number.POSITIVE_INFINITY,
        }),
        minBytes: 100 * 1024 * 1024,
    })

    let fromBlock = await portal.getHead().then((h) => (h?.number ?? 0) - 50_000)

    const db = new Sqlite3('./test.db')

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
        .pipeThrough(finalizer({ref: BlockRef as any}))
        .pipeThrough(createProgressTracker('finalizer'))
        .pipeTo(
            new DataWriter(
                {
                    write: async (batch) => {
                        const head = batch.head
                        const data = batch.data
                        const values = data
                            .map((block) => {
                                const ref = BlockRef.get(block)
                                return `(${ref.number}, ${ref.hash ? `'${ref.hash}'` : 'NULL'}, '${JSON.stringify(
                                    block
                                )}')`
                            })
                            .join(', ')
                        const sql = `INSERT INTO blocks (number, hash, data) VALUES ${values}`
                        db.exec(sql)
                        return head
                    },
                    close: async () => {},
                    fork: async () => undefined,
                },
                BlockRef
            )
        )
}

function createSqliteWriter(db: Sqlite3.Database) {
    db.exec(`CREATE TABLE IF NOT EXISTS blocks (
            number INTEGER PRIMARY KEY,
            hash TEXT,
            data JSONB
        )`)

    const headStmt = db.prepare<[], BlockRef>('SELECT number, hash FROM blocks ORDER BY number DESC LIMIT 1')
    const headsStmt = db.prepare<[number], BlockRef>(
        'SELECT number, hash FROM blocks WHERE number >= ? ORDER BY number ASC'
    )
    const rollbackStmt = db.prepare<[number], void>('DELETE FROM blocks WHERE number >= ?')

    return new DataWriter<SolanaPortalData<any>>(
        {
            init: async () => {
                const head = headStmt.get()
                if (!head) return undefined

                return {
                    number: head.number,
                    hash: head.hash,
                }
            },
            write: async (batch, head) => {
                await new Promise((resolve) => setTimeout(resolve, 0))

                if (batch.data.length > 0) {
                    const values = batch.data
                        .map((block) => {
                            const ref = BlockRef.get(block)
                            return `(${ref.number}, ${ref.hash ? `'${ref.hash}'` : 'NULL'}, '${JSON.stringify(block)}')`
                        })
                        .join(', ')
                    const sql = `INSERT INTO blocks (number, hash, data) VALUES ${values}`
                    db.exec(sql)

                    return BlockRef.get(last(batch.data))
                }

                return head
            },
            fork: async (refs) => {
                const newHead = db.transaction(() => {
                    let rollbackPoint: BlockRef | undefined

                    if (refs.length > 0) {
                        const minBlock = refs[0]
                        const maxBlock = refs[refs.length - 1]
                        const localBlocks = db
                            .prepare<[number], {number: number; hash: string; data: unknown}>(
                                'SELECT number, hash, data FROM blocks WHERE number >= ? ORDER BY number ASC'
                            )
                            .all(minBlock.number)

                        const localBlockMap = new Map(localBlocks.map((block) => [block.number, block]))
                        for (const ref of refs) {
                            const localBlock = localBlockMap.get(ref.number)
                            if (!localBlock || BlockRef.compare(localBlock, ref) !== DataRef.Compare.Equal) {
                                rollbackPoint = ref
                                break
                            }
                        }

                        if (rollbackPoint === undefined) {
                            const currentHead = headStmt.get()
                            if (currentHead && currentHead.number > maxBlock.number) {
                                rollbackPoint = maxBlock
                            }
                        }
                    }

                    if (rollbackPoint !== undefined) {
                        rollbackStmt.run(rollbackPoint.number)
                    }

                    const newHead = headStmt.get()
                    if (!newHead) {
                        throw new Error('unable to rollback')
                    }

                    return newHead
                })()
                return newHead
            },
            close: async () => {},
        },
        BlockRef
    )
}

function createSqliteReader(db: Sqlite3.Database) {
    const headStmt = db.prepare<[], BlockRef>('SELECT number, hash FROM blocks ORDER BY number DESC LIMIT 1')
    const headsStmt = db.prepare<[number], BlockRef & {data: unknown}>(
        'SELECT number, hash, data FROM blocks WHERE number >= ? ORDER BY number ASC LIMIT 10000'
    )
    return new DataReader<SolanaPortalData<any>>(
        {
            init: async () => headStmt.get(),
            read: async function* (req) {
                let head = req.offset

                while (true) {
                    await new Promise((resolve) => setTimeout(resolve, 0))

                    const blocks = db.transaction(() => {
                        const blocks = headsStmt.all(head?.number ?? 0)
                        if (blocks.length === 0) return []

                        //if (head && (blocks[0].number > head.number || blocks[0].hash !== head.hash)) {
                        //    const refs = db
                        //        .prepare<[number], BlockRef>(
                        //            'SELECT number, hash FROM blocks WHERE number <= ? LIMIT 100'
                        //        )
                        //        .all(head.number)
                        //    throw new ForkException(refs)
                        //}

                        return blocks.slice(1)
                    })()
                    if (blocks.length === 0) continue

                    const lastBlock = blocks[blocks.length - 1]
                    head = lastBlock ? lastBlock : head || {number: 0, hash: undefined}

                    yield {
                        head,
                        data: blocks.map((b) => JSON.parse(b.data as string)) as SolanaPortalData<any>['item'][],
                    }
                }
            },
        },
        BlockRef
    )
}

function createProgressTracker(prefix: string) {
    const logger = createLogger(`sqd:${prefix}`)

    return transformer({
        refIn: BlockRef,
        refOut: BlockRef,
        transform: async (batch) => {
            if (batch.data.length > 0) {
                const {data, finalizedHead, head} = batch
                let lastBlock = last(data)
                let lastBlockRef = BlockRef.get(lastBlock)
                logger.info(
                    [
                        `[${new Date().toISOString()}] progress: ${lastBlockRef.number} / ${head?.number}`,
                        `blocks: ${data.length}`,
                        `finalized: ${finalizedHead?.number}`,
                    ].join(', ')
                )
            }
            return batch
        },
    })
}

main()
