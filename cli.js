#!/usr/bin/env node

import ora from 'ora'
import { todo } from './migrate.js'
import { DynamoDBClient } from '@aws-sdk/client-dynamodb'

const src = process.argv.at(2) ?? exit('plz pass source table name')
const dst = process.argv.at(3) ?? exit('plz pass destination table name')
const totalSegments = parseInt(process.argv.at(4) ?? exit('plz pass totalSegments'))
const segment = parseInt(process.argv.at(5) ?? exit('plz pass segment'))
const client = new DynamoDBClient()

const spinner = ora({
  text: `Migrating: ${src} -> ${dst}`
}).start()

// await migrate(src, dst, client, spinner)
await todo(src, dst, segment, totalSegments, client, spinner)

function exit (msg) {
  console.error(msg)
  process.exit(1)
}
