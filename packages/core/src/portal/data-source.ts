import {type Data, type DataCursor, type UnfinalizedDataSource, source} from '../pipeline'
import type {PortalClient} from './client'

export interface PortalDataSourceOptions<T extends Data> {
    portal: PortalClient
    query: any
    cursor: DataCursor<T>
}

export function portalDataSource<T extends Data>(options: PortalDataSourceOptions<T>): UnfinalizedDataSource<T> {
    const {portal, query, cursor} = options

    return source({
        unfinalized: true,
        reader: async (opts) => {
            return {
                read: async () => {
                    return {
                        data: [],
                        head: {},
                        offset: opts.offset,
                        cursor,
                    }
                },
            }
        },
    })
}
