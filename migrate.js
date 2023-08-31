import fs from 'node:fs'
import { pipeline } from 'node:stream/promises'
import batch from 'it-batch'
import { stringify } from 'it-ndjson'
import { DynamoDBClient, ScanCommand, BatchGetItemCommand } from '@aws-sdk/client-dynamodb'
import { unmarshall } from '@aws-sdk/util-dynamodb'
import { fromString } from 'uint8arrays/from-string'
import { digest } from 'multiformats'
import { CID } from 'multiformats/cid'
import ora from 'ora'

const CAR = 0x202

// https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithItems.html#WorkingWithItems.BatchOperations
const BATCH_READ_LIMIT = 100

/**
 * Migrate indexes from the `blocks` table to the `blocks-cars-positions` table
 *
 * We convert this:
 * @typedef {{multihash: string, cars: Array<{offset: number, length: number, car: string}> }} BlocksIndex
 * Into this:
 * @typedef {{blockmultihash: string, carpath: string, length: number, offset: number}} BlocksCarsPosition
 */

/**
* @param {string} src
* @param {string} dst
* @param {number} segment dynamodb scan segment number e.g 0
* @param {number} totalSegments total number of dynamodb scan segments e.g 10
* @param {DynamoDBClient} client
* @param {import('ora').Ora} spinner
*/
export async function todo (src, dst, segment, totalSegments, client = new DynamoDBClient(), spinner = ora({ isSilent: true })) {
  let srcCount = 0
  let dstCount = 0

  await pipeline(
    scanSourceTable(src, client, segment, totalSegments),
    async function * (items) {
      for await (const item of items) {
        srcCount++
        spinner.suffixText = `src: ${srcCount} dst: ${dstCount}`
        yield * transformItem(item)
      }
    },
    (items) => batch(items, BATCH_READ_LIMIT),
    async function * (batches) {
      for await (const batch of batches) {
        yield * checkExists(dst, client, batch)
      }
    },
    /** @param {AsyncIterable<BlocksCarsPosition>} items */
    async function * (items) {
      for await (const item of items) {
        dstCount++
        spinner.suffixText = ` src: ${srcCount} dst: ${dstCount}`
        yield item
      }
    },
    stringify,
    fs.createWriteStream(`migrate-block-index-${totalSegments}-${String(segment).padStart(3, '0')}.ndjson`)
  )
  spinner.succeed()
}

/**
* @param {string} src
* @param {DynamoDBClient} client
* @param {object} [startAt]
*/
export async function * scanSourceTable (src, client, segment = 0, totalSegments = 1) {
  let startAt
  while (true) {
    const cmd = new ScanCommand({
      // Limit: 1000, // natural limit ~3281, to be less than 1MB response size.
      TableName: src,
      ExclusiveStartKey: startAt,
      Segment: segment,
      TotalSegments: totalSegments
    })
    /** @type {import('@aws-sdk/client-dynamodb').ScanCommandOutput} */
    const res = await client.send(cmd)
    for (const item of res.Items) {
      yield unmarshall(item)
    }
    if (res.LastEvaluatedKey) {
      startAt = res.LastEvaluatedKey
    } else {
      return
    }
  }
}

/**
 * Get the batch of items. Only existing ones will be returned in the response.
 * We have to avoid duplicates
 * > ValidationException: Provided list of item keys contains duplicates
 *
 * @param {string} dst table name
 * @param {DynamoDBClient} client
 * @param {BlocksCarsPosition[]} items
 */
export async function * checkExists (dst, client, items) {
  // remove duplicates
  const itemMap = new Map()
  for (const item of items) {
    itemMap.set(`${item.blockmultihash}#${item.carpath}`, item)
  }

  // map items to dynamo GetItem query keys
  const Keys = Array.from(itemMap.values()).map(i => {
    return {
      blockmultihash: { S: i.blockmultihash },
      carpath: { S: i.carpath }
    }
  })

  const cmd = new BatchGetItemCommand({
    RequestItems: {
      [dst]: {
        Keys
      }
    }
  })

  const res = await client.send(cmd)

  /** @type {BlocksCarsPosition[]} */
  const found = (res.Responses[dst] ?? []).map(unmarshall)
  const foundSet = new Set()
  for (const { blockmultihash, carpath } of found) {
    foundSet.add(`${blockmultihash}#${carpath}`)
  }

  for (const item of itemMap.values()) {
    const { blockmultihash, carpath } = item
    const found = foundSet.has(`${blockmultihash}#${carpath}`)
    if (!found) {
      yield item
    }
  }

  if (Object.entries(res.UnprocessedKeys ?? {}).length > 0) {
    console.log('UnprocessedKeys', JSON.stringify(res.UnprocessedKeys))
  }
}

/**
 * @param {BlocksIndex} item
 */
export function * transformItem ({ multihash, cars }) {
  for (const { offset, length, car } of cars) {
    /** @type {BlocksCarsPosition} */
    const transformed = {
      blockmultihash: multihash,
      // carpath: maybeUpgradeCarPath(car) ?? car,
      carpath: car,
      offset,
      length
    }
    yield transformed
  }
}

/**
 * @param {string} oldPath `us-east-2/dotstorage-prod-0/raw/bafybeih5gtgdtn5fdgtessl477drjgs6silcnbbhcghe2be777syctx6le/315318734258440246/ciqcibk35zyehabw3eou3cbuharwvnhuc76uinefvu2lvoyczn4mf4y.car`
 */
export function maybeUpgradeCarPath (oldPath) {
  const [, bucket, ...rest] = oldPath.split('/')

  // e.g dotstorage-staging-0, dotstorage-prod-0, dotstorage-prod-1
  if (!bucket.startsWith('dotstorage-')) {
    // alas we have not migrated yet
    return oldPath
  }

  const oldKey = rest.join('/')
  const newKey = toCarKey(oldKey)
  if (newKey) {
    return `us-west-2/carpark-prod-0/${newKey}`
  }

  // alas we could not upgrade
  return oldPath
}

/**
 * Convert legacy key to car cid key where possible
 * @param {string} key raw/bafybeieiltf3tnfdyvdutyolzhfahphgevnjsso26nulfqxtkptyefq3za/315318734258473269/ciqjxmllx5y73brw6mv3pkvd7sotfk2turkupkq7tsgygrdy2yxibri.car
 * @returns {string | undefined} e.g bagbaieratoywxp3r7wddn4zlw6vkh7e5gkvvhjcvi6vb7henqnchrvroqdcq/bagbaieratoywxp3r7wddn4zlw6vkh7e5gkvvhjcvi6vb7henqnchrvroqdcq.car
 */
export function toCarKey (key) {
  if (!key.endsWith('.car')) {
    return undefined
  }
  const keyParts = key.split('/')
  if (keyParts.at(0) === 'raw') {
    const carName = keyParts.at(-1)
    if (!carName) {
      return undefined
    }
    const carCid = toCarCid(carName.slice(0, -4)) // trim .car suffix
    return `${carCid}/${carCid}.car`
  }
  if (keyParts.at(0)?.startsWith('bag')) {
    // already a carKey
    return keyParts.join('/')
  }
}

/**
 * Convert a base32 (without multibase prefix!) sha256 multihash to a CAR CID
 *
 * @param {string} base32Multihash - e.g ciqjxmllx5y73brw6mv3pkvd7sotfk2turkupkq7tsgygrdy2yxibri
 */
export function toCarCid (base32Multihash) {
  const mh = digest.decode(fromString(base32Multihash, 'base32'))
  return CID.create(1, CAR, mh)
}
