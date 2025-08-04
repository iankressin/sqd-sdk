import {type HttpResponse, type BaseHttpClient, HttpError} from '../http-client'
import {addErrorContext, last, unexpectedCase, wait, withAbort, withErrorContext} from '../internal/misc'
import {createFuture, type Future} from '../internal/async'

export interface PortalClientOptions {
    /**
     * The URL of the portal dataset.
     */
    url: string

    /**
     * Optional custom HTTP client to use.
     */
    http: BaseHttpClient

    /**
     * Minimum number of bytes to return.
     * @default 10_485_760 (10MB)
     */
    minBytes?: number

    /**
     * Maximum number of bytes to return.
     * @default minBytes
     */
    maxBytes?: number

    /**
     * Maximum time between stream data in milliseconds for return.
     * @default 300
     */
    maxIdleTime?: number

    /**
     * Maximum wait time in milliseconds for return.
     * @default 5_000
     */
    maxWaitTime?: number

    /**
     * Interval for polling the head in milliseconds.
     * @default 0
     */
    headPollInterval?: number
}

export interface PortalRequestOptions {
    headers?: HeadersInit
    retryAttempts?: number
    retrySchedule?: number[]
    httpTimeout?: number
    bodyTimeout?: number
    abort?: AbortSignal
}

export interface PortalStreamOptions {
    request?: Omit<PortalRequestOptions, 'abort'>

    minBytes?: number
    maxBytes?: number
    maxIdleTime?: number
    maxWaitTime?: number

    headPollInterval?: number
}

export type BlockRef = {
    readonly hash?: string
    readonly number: number
}

export type PortalStreamData<B> = {
    blocks: B[]
    finalizedHead?: BlockRef
}

export interface PortalStream<B> extends AsyncIterable<PortalStreamData<B>> {}

export type PortalQuery = {
    type: string
    fromBlock?: number
    toBlock?: number
    parentBlockHash?: string
    [key: string]: unknown
}

export class PortalClient {
    private url: URL
    private client: BaseHttpClient
    private headPollInterval: number
    private minBytes: number
    private maxBytes: number
    private maxIdleTime: number
    private maxWaitTime: number

    constructor(options: PortalClientOptions) {
        this.url = new URL(options.url)
        this.client = options.http
        this.headPollInterval = options.headPollInterval ?? 0
        this.minBytes = options.minBytes ?? 10 * 1024 * 1024
        this.maxBytes = options.maxBytes ?? this.minBytes
        this.maxIdleTime = options.maxIdleTime ?? 300
        this.maxWaitTime = options.maxWaitTime ?? 5_000
    }

    private getDatasetUrl(path: string): string {
        let u = new URL(this.url)
        if (this.url.pathname.endsWith('/')) {
            u.pathname += path
        } else {
            u.pathname += `/${path}`
        }
        return u.toString()
    }

    async getHead(options?: PortalRequestOptions): Promise<BlockRef | undefined> {
        const res = await this.client.request<BlockRef>(this.getDatasetUrl('head'), {...options, method: 'GET'})
        return res.body ?? undefined
    }

    async getFinalizedHead(options?: PortalRequestOptions): Promise<BlockRef | undefined> {
        const res = await this.client.request<BlockRef>(this.getDatasetUrl('finalized-head'), {
            ...options,
            method: 'GET',
        })
        return res.body ?? undefined
    }

    getFinalizedStream<Q extends PortalQuery = PortalQuery, R = any>(
        query: Q,
        options?: PortalStreamOptions,
    ): PortalStream<R> {
        let {
            headPollInterval = this.headPollInterval,
            minBytes = this.minBytes,
            maxBytes = this.maxBytes,
            maxIdleTime = this.maxIdleTime,
            maxWaitTime = this.maxWaitTime,
            request = {},
        } = options ?? {}

        return createPortalStream(
            query,
            {
                headPollInterval,
                minBytes,
                maxBytes,
                maxIdleTime,
                maxWaitTime,
                request,
            },
            async (q, o) => this.getStreamRequest('finalized-stream', q, o),
        )
    }

    getStream<Q extends PortalQuery = PortalQuery, R = any>(query: Q, options?: PortalStreamOptions): PortalStream<R> {
        let {
            headPollInterval = this.headPollInterval,
            minBytes = this.minBytes,
            maxBytes = this.maxBytes,
            maxIdleTime = this.maxIdleTime,
            maxWaitTime = this.maxWaitTime,
            request = {},
        } = options ?? {}

        return createPortalStream(
            query,
            {
                headPollInterval,
                minBytes,
                maxBytes,
                maxIdleTime,
                maxWaitTime,
                request,
            },
            async (q, o) => this.getStreamRequest('stream', q, o),
        )
    }

    private async getStreamRequest(path: string, query: PortalQuery, options?: PortalRequestOptions) {
        try {
            let res = await this.client
                .request<ReadableStream | undefined>(this.getDatasetUrl(path), {
                    ...options,
                    method: 'POST',
                    json: query,
                    stream: true,
                })
                .catch(
                    withErrorContext({
                        query: query,
                    }),
                )

            switch (res.status) {
                case 200: {
                    return {
                        finalizedHead: getFinalizedHeadHeader(res.headers),
                        stream: res.body
                            ?.pipeThrough(new TextDecoderStream('utf8'))
                            ?.pipeThrough(new LineSplitStream('\n')),
                    }
                }
                case 204:
                    return undefined
                default:
                    throw unexpectedCase(res.status)
            }
        } catch (e: unknown) {
            if (
                e instanceof HttpError &&
                e.response.status === 409 &&
                query.fromBlock != null &&
                query.parentBlockHash != null &&
                e.response.body.lastBlocks != null
            ) {
                e = new ForkException(e.response.body.lastBlocks, {
                    fromBlock: query.fromBlock,
                    parentBlockHash: query.parentBlockHash,
                })
            }

            throw addErrorContext(e as any, {
                query,
            })
        }
    }
}

function createPortalStream<Q extends PortalQuery = PortalQuery, R = any>(
    query: Q,
    options: Required<PortalStreamOptions>,
    requestStream: (
        query: Q,
        options?: PortalRequestOptions,
    ) => Promise<{finalizedHead?: BlockRef; stream?: ReadableStream<string[]>} | undefined>,
): PortalStream<R> {
    let {headPollInterval, request, ...bufferOptions} = options

    let abortStream = new AbortController()

    let finalizedHead: BlockRef | undefined
    let buffer = new PortalStreamBuffer<R>(bufferOptions)

    async function ingest() {
        let abortSignal = abortStream.signal
        let {fromBlock = 0, toBlock, parentBlockHash} = query

        if (abortSignal.aborted) return
        if (toBlock != null && fromBlock > toBlock) return

        let reader: ReadableStreamDefaultReader<string[]> | undefined
        try {
            while (true) {
                let res = await requestStream(
                    {
                        ...query,
                        fromBlock,
                        parentBlockHash,
                    },
                    {
                        ...request,
                        abort: abortSignal,
                    },
                )

                // we are on head
                if (res == null) {
                    buffer.ready()
                    await wait(headPollInterval, abortSignal)
                    continue
                }

                finalizedHead = res.finalizedHead

                // no data left on this range
                if (res.stream == null) return
                reader = res.stream.getReader()

                while (true) {
                    let data = await withAbort(() => reader!.read(), abortSignal)
                    if (data.done) break
                    if (data.value.length === 0) continue

                    let blocks: R[] = []
                    let bytes = 0

                    for (let line of data.value) {
                        let block = JSON.parse(line) as R

                        blocks.push(block)
                        bytes += line.length
                    }

                    await withAbort(() => buffer.put(blocks, bytes), abortSignal)
                }

                return
            }
        } catch (err) {
            if (abortSignal.aborted || isStreamAbortedError(err)) {
                // ignore
            } else {
                throw err
            }
        } finally {
            reader?.cancel().catch(() => {})
        }
    }

    ingest().then(
        () => buffer.close(),
        (err) => buffer.fail(err),
    )

    return {
        [Symbol.asyncIterator]: () => {
            return {
                next: async () => {
                    const res = await buffer.take()
                    if (res.done) {
                        return {done: true, value: undefined}
                    }

                    return {
                        done: false,
                        value: {
                            blocks: res.value,
                            finalizedHead,
                        },
                    }
                },
                throw: (err) => {
                    abortStream.abort(err)
                    return Promise.resolve({done: true, value: undefined})
                },
                return: (value) => {
                    abortStream.abort()
                    return Promise.resolve({done: true, value})
                },
            }
        },
    }
}

class PortalStreamBuffer<B> {
    private buffer: {blocks: B[]; bytes: number}
    private state: 'open' | 'failed' | 'closed' = 'open'
    private error: unknown

    private readyFuture: Future<void> = createFuture()
    private takeFuture: Future<void> = createFuture()
    private putFuture: Future<void> = createFuture()

    private lastChunkTimestamp = Date.now()
    private idleInterval: ReturnType<typeof setInterval> | undefined

    private minBytes: number
    private maxBytes: number
    private maxIdleTime: number
    private maxWaitTime: number

    constructor(options: {maxWaitTime: number; maxBytes: number; maxIdleTime: number; minBytes: number}) {
        this.maxWaitTime = options.maxWaitTime
        this.minBytes = options.minBytes
        this.maxBytes = Math.max(options.maxBytes, options.minBytes)
        this.maxIdleTime = options.maxIdleTime
        this.buffer = {blocks: [], bytes: 0}
    }

    async take(): Promise<{done: true; value?: undefined} | {value: B[]; done: false}> {
        let waitTimeout = setTimeout(() => {
            this.readyFuture.resolve()
        }, this.maxWaitTime)
        this.readyFuture.promise().finally(() => clearTimeout(waitTimeout))

        await Promise.all([this.readyFuture.promise(), this.putFuture.promise()])

        if (this.state === 'failed') {
            throw this.error
        }

        let value = this.buffer.blocks
        this.buffer = {blocks: [], bytes: 0}

        this.takeFuture.resolve()

        if (this.state === 'closed') {
            return value.length === 0 ? {done: true} : {value, done: false}
        }

        if (value == null) {
            throw new Error('buffer is empty')
        }

        this.takeFuture = createFuture()
        this.putFuture = createFuture()
        this.readyFuture = createFuture()

        return {value, done: false}
    }

    async put(blocks: B[], bytes: number) {
        if (this.state !== 'open') {
            throw new Error('buffer is closed')
        }

        this.lastChunkTimestamp = Date.now()
        if (this.idleInterval == null) {
            this.idleInterval = setInterval(
                () => {
                    if (Date.now() - this.lastChunkTimestamp >= this.maxIdleTime) {
                        this.readyFuture.resolve()
                    }
                },
                Math.ceil(this.maxIdleTime / 3),
            )
            this.readyFuture.promise().finally(() => clearInterval(this.idleInterval))
            this.takeFuture.promise().finally(() => {
                this.idleInterval = undefined
            })
        }

        this.buffer.bytes += bytes
        this.buffer.blocks.push(...blocks)

        this.putFuture.resolve()

        if (this.buffer.bytes >= this.minBytes) {
            this.readyFuture.resolve()
        }

        if (this.buffer.bytes >= this.maxBytes) {
            await this.takeFuture.promise()
        }
    }

    ready() {
        this.readyFuture.resolve()
    }

    close() {
        if (this.state !== 'open') return
        this.state = 'closed'
        this.readyFuture.resolve()
        this.putFuture.reject(new Error('closed'))
        this.takeFuture.resolve()
    }

    fail(err: unknown) {
        if (this.state !== 'open') return
        this.state = 'failed'
        this.error = err
        this.readyFuture.resolve()
        this.putFuture.reject(err as Error)
        this.takeFuture.resolve()
    }
}

class LineSplitStream implements ReadableWritablePair<string[], string> {
    private line = ''
    private transform: TransformStream<string, string[]>

    get readable() {
        return this.transform.readable
    }
    get writable() {
        return this.transform.writable
    }

    constructor(separator: string) {
        this.transform = new TransformStream({
            transform: (chunk, controller) => {
                let lines = chunk.split(separator)
                if (lines.length === 1) {
                    this.line += lines[0]
                } else {
                    let result: string[] = []
                    lines[0] = this.line + lines[0]
                    this.line = lines.pop() || ''
                    result.push(...lines)
                    controller.enqueue(result)
                }
            },
            flush: (controller) => {
                if (this.line) {
                    controller.enqueue([this.line])
                    this.line = ''
                }
                // NOTE: not needed according to the spec, but done the same way in nodejs sources
                controller.terminate()
            },
        })
    }
}

export class ForkException extends Error {
    readonly name = 'ForkError'

    constructor(
        readonly lastBlocks: BlockRef[],
        readonly query: {fromBlock: number; parentBlockHash: string},
    ) {
        let parent = last(lastBlocks)
        super(
            `expected ${query.fromBlock} to have parent ${parent.number}#${query.parentBlockHash}, but got ${parent.number}#${parent.hash}`,
        )
    }
}

export function isForkException(err: unknown): err is ForkException {
    if (err instanceof ForkException) return true
    if (err instanceof Error && err.name === 'ForkError') return true
    return false
}

function getFinalizedHeadHeader(headers: HttpResponse['headers']) {
    let finalizedHeadHash = headers.get('X-Sqd-Finalized-Head-Hash')
    let finalizedHeadNumber = headers.get('X-Sqd-Finalized-Head-Number')

    return finalizedHeadHash != null && finalizedHeadNumber != null
        ? {
              hash: finalizedHeadHash,
              number: Number.parseInt(finalizedHeadNumber),
          }
        : undefined
}

function isStreamAbortedError(err: unknown) {
    if (!(err instanceof Error)) return false
    if (!('code' in err)) return false
    switch (err.code) {
        case 'ABORT_ERR':
        case 'ERR_STREAM_PREMATURE_CLOSE':
        case 'ECONNRESET':
            return true
        default:
            return false
    }
}
