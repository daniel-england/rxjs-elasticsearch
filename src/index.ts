import {
    BulkOptions,
    BulkResponse,
    Client,
    ConfigOptions, Hit,
    SearchParams, SearchResponse
} from 'elasticsearch';
import {from, Observable, EMPTY, OperatorFunction} from "rxjs";
import {bufferCount, expand, map, mergeMap, takeWhile} from "rxjs/operators";

export interface BulkAction {
    action: 'index' | 'update' | 'delete' | 'create';
    _id?: number;
    _type?: string;
    _index?: string;
}

interface BulkActionWithPayload extends BulkAction {
    payload: any
}

export interface IndexBulkAction extends BulkActionWithPayload {
    action: 'update',
    if_seq_no?: number,
    if_primary_term?: number
}

export interface UpdateBulkAction extends BulkActionWithPayload {
    action: 'update',
    payload: any,
    retry_on_conflict?: number;
}

export interface DeleteBulkAction extends BulkAction {
    action: 'delete',
    if_seq_no?: number,
    if_primary_term?: number
}

const hasPayload = (action: BulkAction | BulkActionWithPayload): action is BulkActionWithPayload => {
    return (<BulkActionWithPayload>action).payload !== undefined
};

const toArray = <T extends BulkAction> (bulkAction: T) : Array<any> => {
    if (hasPayload(bulkAction)) {
        const {action, payload, ...metadataPayload} = bulkAction;
        return [{[action]: metadataPayload}, payload];
    } else {
        const {action, ...metadataPayload} = bulkAction;
        return [{[action]: metadataPayload}]
    }
};

export class RxjsEsClient {
    private client: Client;

    constructor(options: ConfigOptions) {
        this.client = new Client(options);
    }

    stream<T>(search: SearchParams, batchSize?: number): Observable<Hit<T>> {
        const scroll = '10s';
        search.scroll = scroll;
        search.size = batchSize || 1000;

        return from(this.client.search(search))
            .pipe(
                expand((res: SearchResponse<T>) => res._scroll_id ? this.client.scroll({
                    scroll: scroll,
                    scrollId: res._scroll_id
                }) : EMPTY),
                takeWhile((res: SearchResponse<T>) => res.hits.total > 0),
                mergeMap((res: SearchResponse<T>) => from(res.hits.hits))
            )
    }

    bulkStream(options?: BulkOptions, batchSize?: number, bufferFunction?: Function): OperatorFunction<BulkAction, BulkResponse> {
        const client = this.client;
        const bufferSize = batchSize || 100;
        return <OperatorFunction<BulkAction, BulkResponse>> function (source: Observable<BulkAction>) {
            return source.pipe(
                bufferFunction ? bufferFunction() : bufferCount(bufferSize),
                map((bulkActions: Array<BulkAction>) => bulkActions
                    .reduce((body: Array<any>, bulkAction: BulkAction) => body.concat(toArray(bulkAction)), [])
                ),
                mergeMap((body: Array<any>) => client.bulk({...options, body}))
            );
        }
    }
}
