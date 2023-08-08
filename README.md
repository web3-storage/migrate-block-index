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

Upgrades the carPath key to `${carCid}/${carCid}.car` style where the source key is prefixed with `/raw` and the source bucket is `dotstorage-prod-*`

## Getting started

You will need:

- `node` v18
- aws credentials exported in env vars

run `cli.js`

```
$ ./cli.js src-table-name dest-table-name
ℹ Migrating: src-table-name -> dest-table-name
```

## Migration details

This will be used to migrate indexes from the `blocks` table to the `blocks-cars-position` table.

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


### Reducing writes

1 dynamo write costs > 5x a read, so try to reduce writes!

- If we have or can derive a car cid key
  - if exists in dest table
    - **skip!**
  - else
    - If the old key exists:
       - **skip!**
    - else
      - **write car cid key**
- else
  - If the old key exists:
       - **skip!**
    - else
      - **write old style key** (as it's all we have)

### References

> Write operation costs $1.25 per million requests.
> Read operation costs $0.25 per million requests.
– https://dashbird.io/knowledge-base/dynamodb/dynamodb-pricing/

>Read consumption: Measured in 4KB increments (rounded up!) for each read operation. This is the amount of data that is read from the table or index... if you read a single 10KB item in DynamoDB, you will consume 3 RCUs (10 / 4 == 2.5, rounded up to 3).
>
> Write consumption: Measured in 1KB increments (also rounded up!) for each write operation. This is the size of the item you're writing / updating / deleting during a write operation... if you write a single 7.5KB item in DynamoDB, you will consume 8 WCUs (7.5 / 1 == 7.5, rounded up to 8).
– https://www.alexdebrie.com/posts/dynamodb-costs/
