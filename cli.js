#!/usr/bin/env node

import ora from 'ora'
import { migrate } from './migrate.js'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'

const src = process.argv.at(2) ?? exit('plz pass source table name')
const dst = process.argv.at(3) ?? exit('plz pass destination table name')
const client = new DynamoDBClient()

const spinner = ora({
  text: `Migrating: ${src} -> ${dst}`
}).info().start()

await migrate(src, dst, client, spinner)

function exit (msg) {
  console.error(msg)
  process.exit(1)
}
