import { DynamoDBClient, ScanCommand } from '@aws-sdk/client-dynamodb'
import { unmarshall } from '@aws-sdk/util-dynamodb'
import { fromString } from 'uint8arrays/from-string'
import { digest } from 'multiformats'
import { CID } from 'multiformats/cid'
import ora from 'ora'

const CAR = 0x202

/**
 * Migrate indexes from the `blocks` table to the `blocks-cars-positions` table
 *
 * We convert this:
 * @typedef {{multihash: string, cars: Array<{offset: number, length: number, car: string}> }} BlocksIndex
 * Into this:
 * @typedef {{blockmultihash: string, carPath: string, length: number, offset: number}} BlocksCarsPosition
 *
 * @param {string} src
 * @param {string} dst
 * @param {DynamoDBClient} client
 * @param {import('ora').Ora} spinner
 */
export async function migrate (src, dst, client = new DynamoDBClient(), spinner = ora({ isSilent: true })) {
  let startAt
  let itemCount = 0
  while (true) {
    spinner.text = `Processed: ${itemCount} LastEvaluatedKey: ${JSON.stringify(startAt)}`
    const cmd = new ScanCommand({
      Limit: 1000, // natural limit ~3281, to be less than 1MB response size.
      TableName: src,
      ExclusiveStartKey: startAt
    })
    const res = await client.send(cmd)
    if (res.Items) {
      const transformed = transformItems((res.Items.map(unmarshall)))
      itemCount += res.Items.length
    }
    if (res.LastEvaluatedKey) {
      startAt = res.LastEvaluatedKey
    } else {
      break // we done
    }
  }
}

/**
 * @param {Array<BlocksIndex>} items
 */
export function transformItems (items) {
  const res = []
  for (const { multihash, cars } of items) {
    for (const { offset, length, car } of cars) {
      /** @type {BlocksCarsPosition} */
      const transformed = {
        blockmultihash: multihash,
        carPath: maybeUpgradeCarPath(car),
        oldPath: car,
        offset,
        length
      }

      // console.log(transformed)
      res.push(transformed)
    }
  }
  return res
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
