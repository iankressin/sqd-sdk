import type * as Solana from '@iankressin/core/portal/solana'
import type {Trues} from '@iankressin/core/internal/selection'
import type {Hex, Simplify} from '@iankressin/core/internal/types'

export type FieldSelection = Solana.FieldSelection

export type BlockPartial<F extends FieldSelection = Trues<FieldSelection>> = Solana.BlockData<F>

export type Block<F extends FieldSelection = Trues<FieldSelection>> = {
    header: BlockHeader<F>
    transactions: Transaction<F>[]
    instructions: Instruction<F>[]
    logs: LogMessage<F>[]
    balances: Balance<F>[]
    tokenBalances: TokenBalance<F>[]
    rewards: Reward<F>[]
}
export type BlockHeader<F extends FieldSelection = Trues<FieldSelection>> = Solana.BlockHeader<F['block'] & {}>
export type Transaction<F extends FieldSelection = Trues<FieldSelection>> = Simplify<
    Solana.Transaction<F['transaction'] & {}> & {
        block: Block<F>
        instructions: Instruction<F>[]
        logs: LogMessage<F>[]
        balances: Balance<F>[]
        tokenBalances: TokenBalance<F>[]
    }
>
export type Instruction<F extends FieldSelection = Trues<FieldSelection>> = Simplify<
    Solana.Instruction<F['instruction'] & {}> &
        ((F['instruction'] & {})['data'] extends true ? {d1: Hex; d2: Hex; d4: Hex; d8: Hex} : Record<never, never>) & {
            block: Block<F>
            transaction?: Transaction<F>
            parent?: Instruction<F>
            inner: Instruction<F>[]
            logs: LogMessage<F>[]
        }
>
export type LogMessage<F extends FieldSelection = Trues<FieldSelection>> = Simplify<
    Solana.LogMessage<F['log'] & {}> & {
        block: Block<F>
        transaction?: Transaction<F>
        instruction?: Instruction<F>
    }
>
export type Balance<F extends FieldSelection = Trues<FieldSelection>> = Simplify<
    Solana.Balance<F['balance'] & {}> & {
        block: Block<F>
        transaction?: Transaction<F>
    }
>
export type TokenBalance<F extends FieldSelection = Trues<FieldSelection>> = Simplify<
    Solana.TokenBalance<F['tokenBalance'] & {}> & {
        block: Block<F>
        transaction?: Transaction<F>
    }
>
export type Reward<F extends FieldSelection = Trues<FieldSelection>> = Solana.Reward<
    F['reward'] & {
        block: Block<F>
    }
>

export const REQUIRED_FIELDS = {
    block: {
        number: true,
        hash: true,
        parentHash: true,
    },
    transaction: {
        transactionIndex: true,
    },
    log: {
        transactionIndex: true,
        instructionAddress: true,
        logIndex: true,
    },
    instruction: {
        transactionIndex: true,
        instructionAddress: true,
    },
    balance: {
        transactionIndex: true,
    },
    tokenBalance: {
        transactionIndex: true,
    },
} as const satisfies Solana.FieldSelection

export type RequiredFieldSelection = typeof REQUIRED_FIELDS
