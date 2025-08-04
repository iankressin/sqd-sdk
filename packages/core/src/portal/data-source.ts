import {type Data, DataSource, type UnfinalizedDataSource} from '../pipeline'
import type {PortalClient} from './client'

export interface PortalDataSourceOptions<T extends Data> {
    portal: PortalClient
    query: any
}

export function portalDataSource<T extends Data>(options: PortalDataSourceOptions<T>): UnfinalizedDataSource<T> {
    const {portal, query} = options

    return new DataSource({
        unfinalized: true,
        reader: async (opts) => {
            return {
                read: async () => {
                    return {
                        data: [],
                        head: {},
                        offset: opts.offset,
                    }
                },
            }
        },
    })
}
