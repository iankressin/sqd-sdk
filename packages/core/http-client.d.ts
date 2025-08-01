/**
 * This is a compatibility redirect for contexts that do not understand package.json exports field.
 */
declare module '@iankresin/core/http-client' {
    export * from '@iankresin/core/lib/http-client/index.d.ts'
}
