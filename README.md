# rxjs-elasticsearch
An elasticsearch client that adds search with scroll-to-end around an rxjs observable as well as making bulk updates an
rxjs operator.

## Stream

``` typescript
import {RxjsEsClient} from 'rxjs-elasticsearch';

const client = new RxjsEsClient();

client.stream({q: 'color:blue'})
    .subscribe(
        next => console.log(JSON.stringify(next)),
        err => console.error('Error streaming search', err),
        () => console.log('Jobs done!!!')
    );
```

## Stream Update

``` typescript
import {RxjsEsClient, IndexBulkAction} from 'rxjs-elasticsearch';
import {map} from 'rxjs/operators';

const client = new RxjsEsClient();

client.stream({q: 'color:blue'})
    .pipe(
        map(hit => ({...hit, {color: 'green'}})),
        map(updated => new IndexBulkAction({_id:updated._id}, updated)),
        client.bulkStream({index: 'widgets', type:'foo'})
    )
    .subscribe(
        next => console.log(JSON.stringify(next)),
        err => console.error('Error streaming search', err),
        () => console.log('Jobs done!!!')
    );
```
