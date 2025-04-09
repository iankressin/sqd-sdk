/**
 * This is a compatibility redirect for contexts that do not understand package.json exports field.
 */
declare module '@sqd-sdk/core/http-client' {
    export * from '@sqd-sdk/core/lib/http-client/index.d.ts'
}
