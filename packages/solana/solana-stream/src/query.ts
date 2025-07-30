import type * as Solana from '@sqd-sdk/core/portal/solana'
import {mergeSelection, type MergeSelection} from '@sqd-sdk/core/internal/selection'
import {applyRangeBound, mergeRangeRequests, type Range, type RangeRequest} from '@sqd-sdk/core/internal/range/index'

// TODO: is it needed?
export {
    DataRequest,
    TransactionRequest,
    InstructionRequest,
    LogRequest,
    BalanceRequest,
    TokenBalanceRequest,
    RewardRequest,
} from '@sqd-sdk/core/portal/solana'

export type RequestOptions<R> = {range?: Range; request: R}
export type LogRequestOptions = RequestOptions<Solana.LogRequest>
export type TransactionRequestOptions = RequestOptions<Solana.TransactionRequest>
export type InstructionRequestOptions = RequestOptions<Solana.InstructionRequest>
export type TokenBalanceRequestOptions = RequestOptions<Solana.TokenBalanceRequest>
export type BalanceRequestOptions = RequestOptions<Solana.BalanceRequest>
export type RewardRequestOptions = RequestOptions<Solana.RewardRequest>

export type SolanaQueryOptions<F extends Solana.FieldSelection = Solana.FieldSelection> = {
    fields: F
    requests: RangeRequest<Solana.DataRequest>[]
}

export class SolanaQueryBuilder<F extends Solana.FieldSelection = {block: {number: true; hash: true}}> {
    private range: Range = {from: 0}
    private requests: RangeRequest<Solana.DataRequest>[] = []
    private fields: F = {
        block: {number: true, hash: true},
    } as F

    private addRequest(type: keyof Solana.DataRequest, options: RequestOptions<any>): this {
        this.requests.push({
            range: options.range ?? {from: 0},
            request: {
                [type]: [mapRequest(options)],
            },
        })
        return this
    }

    includeAllBlocks(range?: Range): this {
        this.requests.push({range: range ?? {from: 0}, request: {includeAllBlocks: true}})
        return this
    }

    addLog(options: LogRequestOptions): this {
        return this.addRequest('logs', options)
    }

    addTransaction(options: TransactionRequestOptions): this {
        return this.addRequest('transactions', options)
    }

    addReward(options: RewardRequestOptions): this {
        return this.addRequest('rewards', options)
    }

    addBalance(options: BalanceRequestOptions): this {
        return this.addRequest('balances', options)
    }

    addTokenBalance(options: TokenBalanceRequestOptions): this {
        return this.addRequest('tokenBalances', options)
    }

    addInstruction(options: InstructionRequestOptions): this {
        return this.addRequest('instructions', options)
    }

    setRange(range: Range): this {
        this.range = range
        return this
    }

    setFields<F extends Solana.FieldSelection>(fields: F): SolanaQueryBuilder<F> {
        this.fields = fields as any
        return this as any
    }

    build(): SolanaQueryOptions<F> {
        let requests = mergeRangeRequests(this.requests, mergeDataRequests)
        return {
            fields: this.fields,
            requests: applyRangeBound(requests, this.range),
        }
    }
}

export function mergeDataRequests(...requests: Solana.DataRequest[]): Solana.DataRequest {
    let res: Solana.DataRequest = {}
    for (let req of requests) {
        res.transactions = concatRequestLists(res.transactions, req.transactions)
        res.logs = concatRequestLists(res.logs, req.logs)
        res.balances = concatRequestLists(res.balances, req.balances)
        res.tokenBalances = concatRequestLists(res.tokenBalances, req.tokenBalances)
        res.rewards = concatRequestLists(res.rewards, req.rewards)
        res.instructions = concatRequestLists(res.instructions, req.instructions)
        if (res.includeAllBlocks || req.includeAllBlocks) {
            res.includeAllBlocks = true
        }
    }
    return res
}

export type MergeQueryOptions<T extends SolanaQueryOptions, U extends SolanaQueryOptions> = SolanaQueryOptions<
    MergeSelection<T['fields'], U['fields']>
> extends infer R
    ? R
    : never

export type MergeQueryOptionsAll<T extends readonly SolanaQueryOptions[]> = T extends readonly [infer F, ...infer R]
    ? F extends SolanaQueryOptions
        ? R extends [SolanaQueryOptions, ...SolanaQueryOptions[]]
            ? MergeQueryOptions<F, MergeQueryOptionsAll<R>>
            : F
        : never
    : never

export function mergeRequests(...requests: RangeRequest<Solana.DataRequest>[]): RangeRequest<Solana.DataRequest>[] {
    return mergeRangeRequests(requests, mergeDataRequests)
}

export function mergeQueries<T extends SolanaQueryOptions, U extends SolanaQueryOptions>(
    a: T,
    b: U
): MergeQueryOptions<T, U>
export function mergeQueries<T extends readonly SolanaQueryOptions[]>(...queries: T): MergeQueryOptionsAll<T>
export function mergeQueries<T extends readonly SolanaQueryOptions[]>(...queries: T) {
    return {
        fields: mergeSelection(...queries.map((q) => q.fields)),
        requests: mergeRequests(...queries.flatMap((q) => q.requests)),
    }
}

function concatRequestLists<T extends object>(a?: T[], b?: T[]): T[] | undefined {
    let result = [...(a || []), ...(b || [])]
    return result.length ? result : undefined
}

function mapRequest<T>(options: RequestOptions<T>): T {
    return {...options.request}
}
