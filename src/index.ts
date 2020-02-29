import {ApiResponse, Client} from '@elastic/elasticsearch';
import {Bulk, Scroll, Search} from '@elastic/elasticsearch/api/requestParams';
import {TransportRequestOptions} from '@elastic/elasticsearch/lib/Transport';
import {EMPTY, from, Observable, OperatorFunction} from 'rxjs';
import {bufferCount, expand, map, mergeMap, takeWhile, tap, take} from 'rxjs/operators';

export type RxBulkRequest = Omit<Bulk, 'body'>;

export interface BulkAction {
    action: 'index' | 'update' | 'delete' | 'create';
    _id?: number;
    _type?: string;
    _index?: string;
}

interface BulkActionWithPayload extends BulkAction {
    payload: any;
}

export interface IndexBulkAction extends BulkActionWithPayload {
    action: 'update';
    if_seq_no?: number;
    if_primary_term?: number;
}

export interface UpdateBulkAction extends BulkActionWithPayload {
    action: 'update';
    payload: any;
    retry_on_conflict?: number;
}

export interface DeleteBulkAction extends BulkAction {
    action: 'delete';
    if_seq_no?: number;
    if_primary_term?: number;
}

interface ShardsResponse {
    total: number;
    successful: number;
    failed: number;
    skipped: number;
}

export interface Explanation {
    value: number;
    description: string;
    details: Explanation[];
}

export interface Hit<T> {
    _index: string;
    _type: string;
    _id: string;
    _score: number;
    _source: T;
    _version?: number;
    _explanation?: Explanation;
    fields?: any;
    highlight?: any;
    inner_hits?: any;
    matched_queries?: string[];
    sort?: string[];
}

interface SearchResponse<T> {
    took: number;
    timed_out: boolean;
    _scroll_id?: string;
    _shards: ShardsResponse;
    hits: {
        total: number;
        max_score: number;
        hits: Array<Hit<T>>;
    };
    aggregations?: any;
}

const hasPayload = (action: BulkAction | BulkActionWithPayload): action is BulkActionWithPayload => {
    return (action as BulkActionWithPayload).payload !== undefined;
};

const toArray = <T extends BulkAction>(bulkAction: T): any[] => {
    if (hasPayload(bulkAction)) {
        const {action, payload, ...metadataPayload} = bulkAction;
        return [{[action]: metadataPayload}, payload];
    } else {
        const {action, ...metadataPayload} = bulkAction;
        return [{[action]: metadataPayload}];
    }
};

export const extend = (client: Client) => {
    client.extend('rxSearch', () => {
        return function rxSearch<T>(params: Search, options?: TransportRequestOptions): Observable<Hit<T>> {
            const scroll = '10s';
            const maxBatchSize = 10;
            const numberToGet = params.size;

            params.scroll = scroll;
            params.size = numberToGet && numberToGet < maxBatchSize ? numberToGet : undefined || maxBatchSize;
            const numBatches = numberToGet ? Math.ceil(numberToGet/params.size) : undefined;

            return from(client.search(params, options))
                .pipe(
                    expand((res: ApiResponse<SearchResponse<T>>) => {
                        return res.body._scroll_id && res.body.hits.hits.length === params.size ?
                            client.scroll({
                                scroll,
                                scrollId: res.body._scroll_id
                        } as Scroll) : EMPTY
                    }),
                    numberToGet ? take(numBatches) : takeWhile((res: ApiResponse<SearchResponse<T>>) => res.body.hits.hits.length > 0),
                    mergeMap((res: ApiResponse<SearchResponse<T>>) => from(res.body.hits.hits)),
                    numberToGet ? take(numberToGet) : tap(() => {})
                );
        };
    });

    client.extend('rxBulk', () => {
        return function bulkStream(params?: RxBulkRequest, options?: TransportRequestOptions, batchSize?: number): OperatorFunction<BulkAction, ApiResponse> {
            const bufferSize = batchSize || 100;
            return (source: Observable<BulkAction>) =>
                source.pipe(
                    bufferCount(bufferSize),
                    map((bulkActions: BulkAction[]) => bulkActions
                        .reduce((body: any[], bulkAction: BulkAction) => body.concat(toArray(bulkAction)), [])
                    ),
                    mergeMap((body: any[]) => client.bulk({...params, body} as Bulk, options))
                );
        };
    });
};
