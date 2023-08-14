import test from 'ava'
import { createDynamo, createDynamoTable } from './_helpers.js'
import { ScanCommand } from '@aws-sdk/client-dynamodb'
import { write, BATCH_WRITE_LIMIT } from '../write.js'
import { unmarshall } from '@aws-sdk/util-dynamodb'

test('write all items', async t => {
  const { client, container } = await createDynamo()
  t.teardown(() => container.stop())

  const dst = await createDynamoTable(client)

  /** @type {import('../write').BlocksCarsPosition[]} */
  const input = Array(BATCH_WRITE_LIMIT + 1).fill(0).map((_, i) => {
    return {
      blockmultihash: `z${i}`,
      carpath: `/region/bucket/raw/sha${i}.car`,
      length: 1000,
      offset: i
    }
  })

  await write(input.map(x => `${JSON.stringify(x)}\n`), dst, 0, 1, client)

  const cmd = new ScanCommand({
    TableName: dst
  })

  const res = await client.send(cmd)

  t.is(res.Items.length, input.length)
  const items = res.Items.map(unmarshall)

  for (const expected of input) {
    const actual = items.find(x => x.blockmultihash === expected.blockmultihash)
    t.like(expected, actual)
  }

  // what happens if we bulk write values that already exist?
  await write(input.map(x => `${JSON.stringify(x)}\n`), dst, 0, 1, client)

  const res2 = await client.send(cmd)

  t.is(res2.Items.length, input.length)
  const items2 = res2.Items.map(unmarshall)

  for (const expected of input) {
    const actual = items2.find(x => x.blockmultihash === expected.blockmultihash)
    t.like(expected, actual)
  }
})
