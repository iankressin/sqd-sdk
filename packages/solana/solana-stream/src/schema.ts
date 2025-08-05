import {
    ANY_OBJECT,
    array,
    B58,
    BIG_NAT,
    BOOLEAN,
    BYTES,
    constant,
    NAT,
    nullable,
    object,
    oneOf,
    option,
    STRING,
    withDefault,
} from '@iankressin/core/validation'
import type {FieldSelection} from './objects'
import {weakMemo} from '@iankressin/core/internal/misc'
import type {Selector} from '@iankressin/core/internal/selection'

export function project<T>(fields: Selector<keyof T> | undefined, obj: T): Partial<T> {
    if (fields == null) return {}
    let result: Partial<T> = {}
    let key: keyof T
    for (key in obj) {
        if (fields[key]) {
            result[key] = obj[key]
        }
    }
    return result
}

export const AddressTableLookup = object({
    accountKey: B58,
    readonlyIndexes: array(NAT),
    writableIndexes: array(NAT),
})

export const getDataSchema = weakMemo((fields: FieldSelection) => {
    let BlockHeader = object({
        number: NAT,
        hash: B58,
        parentHash: B58,
        ...project(fields.block, {
            height: NAT,
            parentSlot: NAT,
            timestamp: NAT,
        }),
    })

    let Transaction = object({
        ...project(fields.transaction, {
            transactionIndex: NAT,
            version: oneOf({
                legacy: constant('legacy'),
                versionNumber: NAT,
            }),
            accountKeys: array(B58),
            addressTableLookups: array(AddressTableLookup),
            numReadonlySignedAccounts: NAT,
            numReadonlyUnsignedAccounts: NAT,
            numRequiredSignatures: NAT,
            recentBlockhash: B58,
            signatures: array(B58),
            err: nullable(ANY_OBJECT),
            computeUnitsConsumed: BIG_NAT,
            fee: BIG_NAT,
            loadedAddresses: option(
                object({
                    readonly: array(B58),
                    writable: array(B58),
                }),
            ),
            hasDroppedLogMessages: BOOLEAN,
        }),
    })

    let Instruction = object({
        ...project(fields.instruction, {
            transactionIndex: NAT,
            instructionAddress: array(NAT),
            programId: B58,
            accounts: array(B58),
            data: B58,
            computeUnitsConsumed: option(BIG_NAT),
            d1: BYTES,
            d2: BYTES,
            d4: BYTES,
            d8: BYTES,
            error: option(STRING),
            isCommitted: BOOLEAN,
            hasDroppedLogMessages: BOOLEAN,
        }),
    })

    let LogMessage = object({
        transactionIndex: NAT,
        logIndex: NAT,
        instructionAddress: array(NAT),
        ...project(fields.log, {
            programId: B58,
            kind: oneOf({
                log: constant('log'),
                data: constant('data'),
                other: constant('other'),
            }),
            message: STRING,
        }),
    })

    let Balance = object({
        ...project(fields.balance, {
            transactionIndex: NAT,
            account: B58,
            pre: BIG_NAT,
            post: BIG_NAT,
        }),
    })

    let TokenBalance = object({
        ...project(fields.tokenBalance, {
            transactionIndex: NAT,
            account: B58,
            preProgramId: option(B58),
            postProgramId: option(B58),
            preMint: option(B58),
            postMint: option(B58),
            preDecimals: option(NAT),
            postDecimals: option(NAT),
            preOwner: option(B58),
            postOwner: option(B58),
            preAmount: option(BIG_NAT),
            postAmount: option(BIG_NAT),
        }),
    })

    let Reward = object({
        ...project(fields.reward, {
            pubkey: B58,
            lamports: BIG_NAT,
            postBalance: BIG_NAT,
            rewardType: option(STRING),
            commission: option(NAT),
        }),
    })

    return object({
        header: BlockHeader,
        transactions: withDefault([], array(Transaction)),
        instructions: option(array(Instruction)),
        logs: option(array(LogMessage)),
        balances: option(array(Balance)),
        tokenBalances: option(array(TokenBalance)),
        rewards: option(array(Reward)),
    })
})
