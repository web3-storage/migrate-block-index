# Migrate block index

This script reads from the source dynamo table and writes to the destination table.

Transform the index records from:
```ts
{ multihash: string, cars: Array<{ offset: number, length: number, car: string }> }
```
to:
```ts
{ blockmultihash: string, carPath: string, length: number, offset: number }
```

## Getting started

You will need:

- `node` v18
- aws credentials exported in env vars

There are two phases: find and transform the indexes, and, write them to destination.

run `cli.js` to find and transform the set of indexes that exist in the source table but not in the destination. 

This will write `migrate-block-index-<totalSegments>-<currentSegment>.ndjson` files to disk with just the subset of indexes that need to be written to the destination table.

```sh
$ ./cli.js src-table-name dest-table-name 859 0
# writes missing indexes to migrate-block-index-859-000.ndjson
```

run `write-cli.js` to write indexes from a file to the destination table.

```sh
$ ./write-cli.js migrate-block-index dest-table-name 859 0
# migrate-block-index-859-000.ndjson -> dest-table-name
```

### Concurrency

Each command takes a `totalSegments` and `segment` arg which is used to split up the initial table scan into `totalSegment` partitions. Use `parallel` to manage concurrency e.g

```sh
# runs 859 jobs from 0 to 858, default parallelization = cpu threads
seq 0 858 | parallel ./cli.js blocks prod-ep-v1-blocks-cars-position 859 {1}
```

## Migration details

This will be used to migrate indexes from the `blocks` table to the `blocks-cars-position` table.

1 dynamo write costs more than 5x a read, so try to reduce writes!

### Source table scan cost

`blocks` table stats

| Item count    | Table size | Average item size
|---------------|------------|-----------------
| 858 million   | 273 GB     | 319 bytes

- 4k / 319 bytes = 12 items per 1 RCU _(eventaully consistent, cheap read, not transaction)_
- 858 million / 12 = 71 million RCUs 
- 71 * $0.25 per million = **$17 for a full table scan**

### Destination table write cost

`blocks-cars-position` table stats

| Item count    | Table size | Average item size
|---------------|------------|-----------------
| 43 billion    | 11 TB      | 255 bytes

Assuming we have to write 1 new record for every source record

- 1kb / 255 bytes = 3 items per WCU
- 858 million / 3 items per WCU = 286 million WCUs
- 286 * $1.25 per million = **$357.5 total write cost**

### Throughput

Scanning ~1M rows and checking if they exist = 100min = 10k per min
859 batches = 1,718hr = 71 days

1 batch = 800MB down 300MB up (~2MB/s down at 10 workers)

max sst "job" length = 8hrs

~10k rows per minute, 15mins lambda runtime.


### References

> Write operation costs $1.25 per million requests.
> Read operation costs $0.25 per million requests.
– https://dashbird.io/knowledge-base/dynamodb/dynamodb-pricing/

>Read consumption: Measured in 4KB increments (rounded up!) for each read operation. This is the amount of data that is read from the table or index... if you read a single 10KB item in DynamoDB, you will consume 3 RCUs (10 / 4 == 2.5, rounded up to 3).
>
> Write consumption: Measured in 1KB increments (also rounded up!) for each write operation. This is the size of the item you're writing / updating / deleting during a write operation... if you write a single 7.5KB item in DynamoDB, you will consume 8 WCUs (7.5 / 1 == 7.5, rounded up to 8).
– https://www.alexdebrie.com/posts/dynamodb-costs/

> If a ConditionExpression evaluates to false during a conditional write, DynamoDB still consumes write capacity from the table. The amount consumed is dependent on the size of the item (whether it’s an existing item in the table or a new one you are attempting to create or update). For example, if an existing item is 300kb and the new item you are trying to create or update is 310kb, the write capacity units consumed will be the 310kb item.
– https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html#WorkingWithItems.ConditionalUpdate

