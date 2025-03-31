import type {Simplify} from './types/misc'

export type Selector<Props extends string | number | symbol = string> = {
    [P in Props]?: boolean
}

export type Select<T, S> = S extends never
    ? never
    : Simplify<{
          [K in keyof T as K extends keyof S ? (S[K] extends true ? K : never) : never]: T[K]
      }>

export type Selection = {
    [P in string]?: boolean | Selection
}

type MergeSelectionValues<T, U> = T extends true
    ? true
    : U extends true
      ? true
      : T extends Selection
        ? U extends Selection
            ? MergeSelection_<T, U>
            : never
        : never

// NOTE: doesn't work well without condition
type MergeSelection_<T, U> = T extends any
    ? U extends any
        ? Simplify<{
              [K in keyof T | keyof U as (K extends keyof T ? T[K] : unknown) extends true | Selection
                  ? K
                  : (K extends keyof U ? U[K] : unknown) extends true | Selection
                    ? K
                    : never]: K extends keyof T
                  ? K extends keyof U
                      ? MergeSelectionValues<T[K], U[K]>
                      : T[K]
                  : K extends keyof U
                    ? U[K]
                    : never
          }>
        : never
    : never

export type MergeSelection<T extends Selection, U extends Selection> = MergeSelection_<T, U>

export type MergeSelectionAll<T extends readonly Selection[]> = T extends readonly [infer F, ...infer R]
    ? F extends Selection
        ? R extends [Selection, ...Selection[]]
            ? MergeSelection<F, MergeSelectionAll<R>>
            : F
        : never
    : never

export type Trues<T extends Selection> = Simplify<{
    [K in keyof T]-?: [T[K] & {}] extends [Selection] ? Trues<T[K] & {}> : true
}>

export function mergeSelection<T extends Selection, U extends Selection>(a: T, b: U): MergeSelection<T, U>
export function mergeSelection<T extends readonly Selection[]>(...selections: T): MergeSelectionAll<T>
export function mergeSelection(...selections: Selection[]) {
    let res: Selection = {}

    for (let selection of selections) {
        for (let key in selection) {
            if (res[key] == null) {
                res[key] = selection[key]
            } else if (res[key] === true || selection[key] === true) {
                res[key] = true
            } else if (typeof res[key] === 'object' && typeof selection[key] === 'object') {
                res[key] = mergeSelection(res[key], selection[key])
            }
        }
    }

    return res
}
