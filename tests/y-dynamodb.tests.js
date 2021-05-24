import * as t from 'lib0/testing.js'
const Y = require('yjs')
const { DynamoDBClient } = require("@aws-sdk/client-dynamodb");
const DynamoDbPersistence = require('./y-dynamodb')
const { PREFERRED_TRIM_SIZE, getDynamoDbUpdates } = DynamoDbPersistence

const config = {
  aws: {
    region: 'us-west-2',
    endpoint: 'http://localhost:8000'
  },
  tableName: 'test-y-dynamodb'
}

/**
 * Flushes all updates to ldb and delets items from updates array.
 *
 * @param {dynamoDbPersistence} ldb
 * @param {string} docName
 * @param {Array<Uint8Array>} updates
 */
const flushUpdatesHelper = (ldb, docName, updates) =>
  Promise.all(updates.splice(0).map(update => ldb.storeUpdate(docName, update)))

/**
 * @param {t.TestCase} tc
 */
// export const testLeveldbUpdateStorage = async tc => {
//   const docName = tc.testName
//   const ydoc1 = new Y.Doc()
//   ydoc1.clientID = 0 // so we can check the state vector
//   const dynamoDbPersistence = DynamoDbPersistence(config)
//   // clear all data, so we can check allData later
//   await dynamoDbPersistence.transact(async db => db.clear())
//   t.compareArrays([], await dynamoDbPersistence.getAllDocNames())
//
//   const updates = []
//
//   ydoc1.on('update', update => {
//     updates.push(update)
//   })
//
//   ydoc1.getArray('arr').insert(0, [1])
//   ydoc1.getArray('arr').insert(0, [2])
//
//   await flushUpdatesHelper(dynamoDbPersistence, docName, updates)
//
//   const encodedSv = await dynamoDbPersistence.getStateVector(docName)
//   const sv = decodeStateVector(encodedSv)
//   t.assert(sv.size === 1)
//   t.assert(sv.get(0) === 2)
//
//   const ydoc2 = await dynamoDbPersistence.getYDoc(docName)
//   t.compareArrays(ydoc2.getArray('arr').toArray(), [2, 1])
//
//   const allData = await dynamoDbPersistence.transact(async db => getLevelBulkData(db, { gte: ['v1'], lt: ['v2'] }))
//   t.assert(allData.length > 0, 'some data exists')
//
//   t.compareArrays([docName], await dynamoDbPersistence.getAllDocNames())
//   await dynamoDbPersistence.clearDocument(docName)
//   t.compareArrays([], await dynamoDbPersistence.getAllDocNames())
//   const allData2 = await dynamoDbPersistence.transact(async db => getLevelBulkData(db, { gte: ['v1'], lt: ['v2'] }))
//   console.log(allData2)
//   t.assert(allData2.length === 0, 'really deleted all data')
//
//   await dynamoDbPersistence.destroy()
// }

/**
 * @param {t.TestCase} tc
 */
export const testEncodeManyUpdates = async tc => {
  const N = PREFERRED_TRIM_SIZE * 7
  const docName = tc.testName
  const ydoc1 = new Y.Doc()
  ydoc1.clientID = 0 // so we can check the state vector
  const dynamoDbPersistence = DynamoDbPersistence(config)
  await dynamoDbPersistence.clearDocument(docName)

  const updates = []

  ydoc1.on('update', update => {
    updates.push(update)
  })
  await flushUpdatesHelper(dynamoDbPersistence, docName, updates)

  const keys = await dynamoDbPersistence.transact(db => getDynamoDbUpdates(db, config.tableName, docName))

  for (let i = 0; i < keys.length; i++) {
    t.assert(keys.ykeysort[i][3] === i)
  }

  const yarray = ydoc1.getArray('arr')
  for (let i = 0; i < N; i++) {
    yarray.insert(0, [i])
  }
  await flushUpdatesHelper(dynamoDbPersistence, docName, updates)

  const ydoc2 = await dynamoDbPersistence.getYDoc(docName)
  t.assert(ydoc2.getArray('arr').length === N)

  await dynamoDbPersistence.flushDocument(docName)
  const mergedKeys = await dynamoDbPersistence.transact(db => getDynamoDbUpdates(db, config.tableName, docName))
  t.assert(mergedKeys.length === 1)

  // getYDoc still works after flush/merge
  const ydoc3 = await dynamoDbPersistence.getYDoc(docName)
  t.assert(ydoc3.getArray('arr').length === N)

  // test if state vector is properly generated
  t.compare(Y.encodeStateVector(ydoc1), await dynamoDbPersistence.getStateVector(docName))
  // add new update so that sv needs to be updated
  ydoc1.getArray('arr').insert(0, ['new'])
  await flushUpdatesHelper(dynamoDbPersistence, docName, updates)
  t.compare(Y.encodeStateVector(ydoc1), await dynamoDbPersistence.getStateVector(docName))

  // await dynamoDbPersistence.destroy()
}

/**
 * @param {t.TestCase} tc
 */
export const testDiff = async tc => {
  const N = PREFERRED_TRIM_SIZE * 2 // primes are awesome - ensure that the document is at least flushed once
  const docName = tc.testName
  const ydoc1 = new Y.Doc()
  ydoc1.clientID = 0 // so we can check the state vector
  const dynamoDbPersistence = DynamoDbPersistence(config)
  await dynamoDbPersistence.clearDocument(docName)

  const updates = []
  ydoc1.on('update', update => {
    updates.push(update)
  })

  const yarray = ydoc1.getArray('arr')
  // create N changes
  for (let i = 0; i < N; i++) {
    yarray.insert(0, [i])
  }
  await flushUpdatesHelper(dynamoDbPersistence, docName, updates)

  // create partially merged doc
  const ydoc2 = await dynamoDbPersistence.getYDoc(docName)

  // another N updates
  for (let i = 0; i < N; i++) {
    yarray.insert(0, [i])
  }
  await flushUpdatesHelper(dynamoDbPersistence, docName, updates)

  // apply diff to doc
  const diffUpdate = await dynamoDbPersistence.getDiff(docName, Y.encodeStateVector(ydoc2))
  Y.applyUpdate(ydoc2, diffUpdate)

  t.assert(ydoc2.getArray('arr').length === ydoc1.getArray('arr').length)
  t.assert(ydoc2.getArray('arr').length === N * 2)

  // await dynamoDbPersistence.destroy()
}

/**
 * @param {t.TestCase} tc
 */
export const testMetas = async tc => {
  const docName = tc.testName
  const dynamoDbPersistence = DynamoDbPersistence(config)
  await dynamoDbPersistence.clearDocument(docName)

  await dynamoDbPersistence.setMeta(docName, 'a', 4)
  await dynamoDbPersistence.setMeta(docName, 'a', 5)
  await dynamoDbPersistence.setMeta(docName, 'b', 4)
  const a = await dynamoDbPersistence.getMeta(docName, 'a')
  const b = await dynamoDbPersistence.getMeta(docName, 'b')
  t.assert(a === 5)
  t.assert(b === 4)
  const metas = await dynamoDbPersistence.getMetas(docName)
  t.assert(metas.size === 2)
  t.assert(metas.get('a') === 5)
  t.assert(metas.get('b') === 4)
  await dynamoDbPersistence.delMeta(docName, 'a')
  const c = await dynamoDbPersistence.getMeta(docName, 'a')
  t.assert(c === undefined)
  await dynamoDbPersistence.clearDocument(docName)
  const metasEmpty = await dynamoDbPersistence.getMetas(docName)
  t.assert(metasEmpty.size === 0)

  // await dynamoDbPersistence.destroy()
}

/**
 * @param {t.TestCase} tc
 */
// export const testDeleteEmptySv = async tc => {
//   const docName = tc.testName
//   const dynamoDbPersistence = DynamoDbPersistence(config)
//   await dynamoDbPersistence.clearAll()
//
//   const ydoc = new Y.Doc()
//   ydoc.clientID = 0
//   ydoc.getArray('arr').insert(0, [1])
//   const singleUpdate = Y.encodeStateAsUpdate(ydoc)
//
//   t.compareArrays([], await dynamoDbPersistence.getAllDocNames())
//   await dynamoDbPersistence.storeUpdate(docName, singleUpdate)
//   t.compareArrays([docName], await dynamoDbPersistence.getAllDocNames())
//   const docSvs = await dynamoDbPersistence.getAllDocStateVecors()
//   t.assert(docSvs.length === 1)
//   t.compare([{ name: docName, clock: 0, sv: Y.encodeStateVector(ydoc) }], docSvs)
//
//   await dynamoDbPersistence.clearDocument(docName)
//   t.compareArrays([], await dynamoDbPersistence.getAllDocNames())
//   await dynamoDbPersistence.destroy()
// }

export const testMisc = async tc => {
  const docName = tc.testName
  const dynamoDbPersistence = DynamoDbPersistence(config)
  await dynamoDbPersistence.clearDocument(docName)

  const sv = await dynamoDbPersistence.getStateVector('does not exist')
  t.assert(sv.byteLength === 1)

  // await dynamoDbPersistence.destroy()
}

export const testGetDbInstance = async () => {
  const dynamoDbPersistence = DynamoDbPersistence(config)
  const dbInstance = dynamoDbPersistence.getDbInstance();

  t.assert(dbInstance instanceof DynamoDBClient);
}
