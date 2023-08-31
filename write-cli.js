import { DynamoDBClient } from '@aws-sdk/client-dynamodb'
import { createDynamo, createDynamoTable } from './test/_helpers.js'
import { write } from './write.js'
import fs from 'node:fs'
import ora from 'ora'

const srcPrefix = process.argv.at(2) ?? exit('plz pass src file prefix')
const dst = process.argv.at(3) ?? exit('plz pass destination table name')
const totalSegments = parseInt(process.argv.at(4) ?? exit('plz pass totalSegments'))
const segment = parseInt(process.argv.at(5) ?? exit('plz pass segment'))
const local = process.argv.at(6) === '--local'
const client = await createDbClient(local, dst)

const src = `${srcPrefix}-${totalSegments}-${String(segment).padStart(3, '0')}.ndjson`
const srcStream = fs.createReadStream(src)

const spinner = ora({
  text: `${src} -> ${dst}`
}).start()

await write(srcStream, dst, segment, totalSegments, client, spinner)

function exit (msg) {
  console.error(msg)
  process.exit(1)
}

async function createDbClient (local, dst) {
  if (!local) {
    return new DynamoDBClient()
  }
  const { client } = await createDynamo()
  await createDynamoTable(client, dst)
  return client
}
