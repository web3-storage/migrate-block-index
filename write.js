import { DynamoDBClient, BatchWriteItemCommand } from '@aws-sdk/client-dynamodb'
import { marshall } from '@aws-sdk/util-dynamodb'
import { pipeline } from 'node:stream/promises'
import * as ndjson from 'it-ndjson'
import batch from 'it-batch'
import fs from 'node:fs'
import ora from 'ora'

// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html#WorkingWithItems.BatchOperations
export const BATCH_WRITE_LIMIT = 25

/**
 * Write indexes to the `blocks-cars-positions` table
 *
 * @typedef {{blockmultihash: string, carpath: string, length: number, offset: number}} BlocksCarsPosition
 *
 * @param {AsyncIterable<string>} srcStream
 * @param {string} dst table name
 * @param {number} segment dynamodb scan segment number e.g 0
 * @param {number} totalSegments total number of dynamodb scan segments e.g 10
 * @param {DynamoDBClient} client
 * @param {import('ora').Ora} spinner
 */
export async function write (srcStream, dst, segment, totalSegments, client = new DynamoDBClient(), spinner = ora({ isSilent: true })) {
  let srcCount = 0
  let dstCount = 0

  await pipeline(
    srcStream,
    ndjson.parse,
    (items) => batch(items, BATCH_WRITE_LIMIT),

    /** @param {AsyncIterable<Array<BlocksCarsPosition>>} batches */
    async function * (batches) {
      for await (const batch of batches) {
        srcCount += batch.length
        spinner.suffixText = `src: ${srcCount} dst: ${dstCount}`

        // remove duplicates
        const itemMap = new Map()
        for (const item of batch) {
          itemMap.set(`${item.blockmultihash}#${item.carpath}`, item)
        }

        /** @type {Array<import('@aws-sdk/client-dynamodb').PutRequest} */
        const puts = Array.from(itemMap.values()).map(item => {
          return {
            PutRequest: {
              Item: marshall(item)
            }
          }
        })

        const cmd = new BatchWriteItemCommand({
          RequestItems: {
            [dst]: puts
          }
        })
        const res = await client.send(cmd)

        /** @type {Array<import('@aws-sdk/client-dynamodb').PutRequest} */
        const unprocessed = res.UnprocessedItems[dst] ?? []

        if (unprocessed > 0) {
          console.log('UnprocessedItems', unprocessed.length)
          dstCount += puts.length - unprocessed.length
          for (const item of unprocessed) {
            yield item
          }
        } else {
          dstCount += puts.length
        }
        spinner.suffixText = `src: ${srcCount} dst: ${dstCount}`
      }
    },

    ndjson.stringify,
    fs.createWriteStream(`./write-unprocessed-${totalSegments}-${segment}.ndjson`)
  )

  spinner.succeed()
}
