export type Simplify<T> = {[K in keyof T]: T[K]} & {}

export type Distribute<T> = T extends any ? T : never

export type AddPrefix<Prefix extends string, S> = S extends string ? `${Prefix}${Capitalize<S>}` : never

export type AddKeysPrefix<Prefix extends string, T> = {
    [K in keyof T as AddPrefix<Prefix, K>]: T[K]
}

export type RemovePrefix<Prefix extends string, T> = T extends `${Prefix}${infer S}` ? Uncapitalize<S> : never

export type RemoveKeysPrefix<Prefix extends string, T> = {
    [K in keyof T as RemovePrefix<Prefix, K>]: T[K]
}

export type ConditionalKeys<T, V> = {
    [Key in keyof T]-?: T[Key] extends V ? (T[Key] extends never ? (V extends never ? Key : never) : Key) : never
}[keyof T]

export type ConditionalPick<T, V> = Simplify<Pick<T, ConditionalKeys<T, V>>>

export type ConditionalOmit<T, V> = Simplify<Omit<T, ConditionalKeys<T, V>>>
