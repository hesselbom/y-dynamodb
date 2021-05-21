const Y = require('yjs')
const {
  BatchWriteItemCommand,
  CreateTableCommand,
  DeleteItemCommand,
  DynamoDBClient,
  GetItemCommand,
  PutItemCommand,
  QueryCommand
} = require('@aws-sdk/client-dynamodb')
const encoding = require('lib0/dist/encoding.cjs')
const decoding = require('lib0/dist/decoding.cjs')
const binary = require('lib0/dist/binary.cjs')
const buffer = require('lib0/dist/buffer.cjs')

const PREFERRED_TRIM_SIZE = 500
const YEncodingString = 0
const YEncodingUint32 = 1

const createTable = (db, config) => (
  new Promise((resolve, reject) => {
    const params = {
      TableName: config.tableName,
      KeySchema: [
        { AttributeName: 'ydocname', KeyType: 'HASH' }, // primary key
        { AttributeName: 'ykeysort', KeyType: 'RANGE' } // Sort key
      ],
      AttributeDefinitions: [
        { AttributeName: 'ydocname', AttributeType: 'S' },
        { AttributeName: 'ykeysort', AttributeType: 'B' }
      ],
      ProvisionedThroughput: { ReadCapacityUnits: 5, WriteCapacityUnits: 5 }
    }

    const command = new CreateTableCommand(params)
    db.send(command, (err, data) => {
      /* istanbul ignore next */
      if (err) {
        if (err.name === 'ResourceInUseException') {
          console.log('Table already exists.')
          resolve()
        } else {
          console.error('Unable to create table. Error JSON:', JSON.stringify(err, null, 2))
          reject(err)
        }
      } else {
        console.log('Created table.')
        resolve()
      }
    })
  })
)

const createDocumentUpdateKey = (docName, clock) => ['v1', docName, 'update', clock]
const createDocumentMetaKey = (docName, metaKey) => ['v1', docName, 'meta', metaKey]
const createDocumentMetaEndKey = (docName) => ['v1', docName, 'metb'] // simple trick
const createDocumentStateVectorKey = (docName) => ['v1_sv', docName]
const createDocumentFirstKey = (docName) => ['v1', docName]
const createDocumentLastKey = (docName) => ['v1', docName, 'zzzzzzz']

const writeUint32BigEndian = (encoder, num) => {
  for (let i = 3; i >= 0; i--) {
    encoding.write(encoder, (num >>> (8 * i)) & binary.BITS8)
  }
}

const readUint32BigEndian = decoder => {
  const uint =
    (decoder.arr[decoder.pos + 3] +
    (decoder.arr[decoder.pos + 2] << 8) +
    (decoder.arr[decoder.pos + 1] << 16) +
    (decoder.arr[decoder.pos] << 24)) >>> 0
  decoder.pos += 4
  return uint
}

const valueEncoding = {
  encode: /** @param {Uint8Array} buf */ buf => Buffer.from(buf),
  decode: /** @param {Buffer} buf */ buf => Uint8Array.from(buf)
}

const keyEncoding = {
  encode: /** @param {Array<string|number>} arr */  arr => {
    const encoder = encoding.createEncoder()
    for (let i = 0; i < arr.length; i++) {
      const v = arr[i]
      if (typeof v === 'string') {
        encoding.writeUint8(encoder, YEncodingString)
        encoding.writeVarString(encoder, v)
      } else /* istanbul ignore else */ if (typeof v === 'number') {
        encoding.writeUint8(encoder, YEncodingUint32)
        writeUint32BigEndian(encoder, v)
      } else {
        throw new Error('Unexpected key value')
      }
    }
    return Buffer.from(encoding.toUint8Array(encoder))
  },
  decode: /** @param {Uint8Array} buf */ buf => {
    const decoder = decoding.createDecoder(buf)
    const key = []
    while (decoding.hasContent(decoder)) {
      switch (decoding.readUint8(decoder)) {
        case YEncodingString:
          key.push(decoding.readVarString(decoder))
          break
        case YEncodingUint32:
          key.push(readUint32BigEndian(decoder))
          break
      }
    }
    return key
  }
}

/**
 * For now this is a helper method that creates a Y.Doc and then re-encodes a document update.
 * In the future this will be handled by Yjs without creating a Y.Doc (constant memory consumption).
 *
 * @param {Array<Uint8Array>} updates
 * @return {{update:Uint8Array, sv: Uint8Array}}
 */
const mergeUpdates = (updates) => {
  const ydoc = new Y.Doc()
  ydoc.transact(() => {
    for (let i = 0; i < updates.length; i++) {
      Y.applyUpdate(ydoc, updates[i].value)
    }
  })
  return { update: Y.encodeStateAsUpdate(ydoc), sv: Y.encodeStateVector(ydoc) }
}

/**
 * @param {any} db
 * @param {string} tableName
 * @param {string} docName
 * @return {Promise<number>} Returns -1 if this document doesn't exist yet
 */
const getCurrentUpdateClock = (db, tableName, docName) => new Promise((resolve, reject) => {
  // Get latest update key for doc
  const params = {
    TableName: tableName,
    KeyConditionExpression: 'ydocname = :docName and ykeysort between :id1 and :id2',
    ExpressionAttributeValues: {
      ':docName': { S: docName },
      ':id1': { B: keyEncoding.encode(createDocumentUpdateKey(docName, 0)) },
      ':id2': { B: keyEncoding.encode(createDocumentUpdateKey(docName, binary.BITS32)) }
    },
    ProjectionExpression: 'ykeysort',
    Limit: 1,
    ScanIndexForward: false
  }

  const command = new QueryCommand(params)
  db.send(command, (err, data) => {
    /* istanbul ignore if */
    if (err) {
      reject(err)
    } else {
      if (data.Items.length === 0) {
        resolve(-1)
      } else {
        resolve(keyEncoding.decode(data.Items[0].ykeysort.B)[3])
      }
    }
  })
})

/**
 * Get all document meta for a specific document.
 *
 * @param {any} db
 * @param {string} tableName
 * @param {string} docName
 * @param {any} [opts]
 * @return {Promise<Array<Buffer>>}
 */
const getDynamoDbMetaData = (db, tableName, docName) => new Promise((resolve, reject) => {
  const params = {
    TableName: tableName,
    KeyConditionExpression: 'ydocname = :docName and ykeysort between :id1 and :id2',
    ExpressionAttributeValues: {
      ':docName': { S: docName },
      ':id1': { B: keyEncoding.encode(createDocumentMetaKey(docName, '')) },
      ':id2': { B: keyEncoding.encode(createDocumentMetaEndKey(docName)) }
    },
    // ProjectionExpression: 'ykeysort',
    ScanIndexForward: true
  }

  const command = new QueryCommand(params)
  db.send(command, (err, data) => {
    /* istanbul ignore if */
    if (err) {
      reject(err)
    } else {
      resolve(data.Items.map(item => ({
        ykeysort: keyEncoding.decode(item.ykeysort.B),
        value: valueEncoding.decode(item.value.B)
      })))
    }
  })
})

/**
 * Get all document updates for a specific document.
 *
 * @param {any} db
 * @param {string} tableName
 * @param {string} docName
 * @param {any} [opts]
 * @return {Promise<Array<Buffer>>}
 */
const getDynamoDbUpdates = (db, tableName, docName) => new Promise((resolve, reject) => {
  const params = {
    TableName: tableName,
    KeyConditionExpression: 'ydocname = :docName and ykeysort between :id1 and :id2',
    ExpressionAttributeValues: {
      ':docName': { S: docName },
      ':id1': { B: keyEncoding.encode(createDocumentUpdateKey(docName, 0)) },
      ':id2': { B: keyEncoding.encode(createDocumentUpdateKey(docName, binary.BITS32)) }
    },
    // ProjectionExpression: 'ykeysort',
    ScanIndexForward: true
  }

  const command = new QueryCommand(params)
  db.send(command, (err, data) => {
    /* istanbul ignore if */
    if (err) {
      reject(err)
    } else {
      resolve(data.Items.map(item => ({
        ykeysort: keyEncoding.decode(item.ykeysort.B),
        value: valueEncoding.decode(item.value.B)
      })))
    }
  })
})

/**
 * @param {any} db
 * @param {string} tableName
 * @param {string} docName
 * @param {any} key
 */
const dynamoDbDelete = (db, tableName, docName, key) => new Promise((resolve, reject) => {
  const params = {
    Key: {
      ydocname: { S: docName },
      ykeysort: { B: keyEncoding.encode(key) }
    },
    TableName: tableName
  }

  const command = new DeleteItemCommand(params)
  db.send(command, (err, data) => {
    /* istanbul ignore if */
    if (err) {
      reject(err)
    } else {
      resolve(true)
    }
  })
})

/**
 * @param {any} db
 * @param {string} tableName
 * @param {string} docName
 * @param {any} key
 */
const dynamoDbGet = (db, tableName, docName, key) => new Promise((resolve, reject) => {
  const params = {
    Key: {
      ydocname: { S: docName },
      ykeysort: { B: keyEncoding.encode(key) }
    },
    TableName: tableName
  }

  const command = new GetItemCommand(params)
  db.send(command, (err, data) => {
    /* istanbul ignore if */
    if (err) {
      reject(err)
    } else {
      const buf = data && data.Item && data.Item.value && data.Item.value.B
      if (!buf) resolve(null)
      else resolve(valueEncoding.decode(buf))
    }
  })
})

/**
 * @param {any} db
 * @param {String} tableName
 * @param {String} docName
 * @param {any} key
 * @param {Uint8Array} val
 */
const dynamoDbPut = (db, tableName, docName, key, val) => new Promise((resolve, reject) => {
  const params = {
    TableName: tableName,
    ReturnConsumedCapacity: 'TOTAL',
    Item: {
      ydocname: { S: docName },
      ykeysort: { B: keyEncoding.encode(key) },
      value: { B: valueEncoding.encode(val) }
    }
  }

  const command = new PutItemCommand(params)
  db.send(command, (err, data) => {
    /* istanbul ignore if */
    if (err) {
      console.error('Unable to add item. Error JSON:', JSON.stringify(err, null, 2))
      reject(err)
    } else {
      // console.log('Added item:', JSON.stringify(data, null, 2))
      resolve()
    }
  })
})

/**
 * @param {any} db
 * @param {string} tableName
 * @param {string} docName
 * @param {Array<string|number>} gte Greater than or equal
 * @param {Array<string|number>} lt lower than (not equal)
 * @return {Promise<void>}
 */
const clearRange = (db, tableName, docName, gte, lt) => new Promise((resolve, reject) => {
  // Get all items
  const ltEncoded = keyEncoding.encode(lt)
  const params = {
    TableName: tableName,
    KeyConditionExpression: 'ydocname = :docName and ykeysort between :id1 and :id2',
    ExpressionAttributeValues: {
      ':docName': { S: docName },
      ':id1': { B: keyEncoding.encode(gte) },
      ':id2': { B: ltEncoded }
    },
    ProjectionExpression: 'ykeysort',
    ScanIndexForward: true
  }

  const command = new QueryCommand(params)
  db.send(command, (err, data) => {
    /* istanbul ignore if */
    if (err) {
      reject(err)
    } else {
      if (data.Items.length === 0) {
        resolve()
        return
      }

      // DynamoDB only allows a maximum of 25 items in bulk updates
      // So need to chunk list of items
      const chunkSize = 25
      const promises = []
      for (let i = 0; i < data.Items.length; i += chunkSize) {
        // Bulk delete items
        const batchParams = { RequestItems: { [tableName]: [] } }
        for (const item of data.Items.slice(i, i + chunkSize)) {
          // lt !== lte so ignore if equal
          if (Buffer.compare(item.ykeysort.B, ltEncoded) === 0) {
            continue
          }

          batchParams.RequestItems[tableName].push({
            DeleteRequest: {
              Key: {
                ydocname: { S: docName },
                ykeysort: item.ykeysort
              }
            }
          })
        }

        if (batchParams.RequestItems[tableName].length === 0) continue

        // eslint-disable-next-line
        promises.push(new Promise((resolve2, reject2) => {
          const command = new BatchWriteItemCommand(batchParams)
          db.send(command, (err2, data2) => {            /* istanbul ignore if */
            if (err2) {
              reject2(err2)
            } else {
              resolve2()
            }
          })
        }))
      }

      Promise.all(promises)
        .then(resolve)
        .catch(reject)
    }
  })
})

/**
 * @param {any} db
 * @param {string} tableName
 * @param {string} docName
 * @param {number} from Greater than or equal
 * @param {number} to lower than (not equal)
 * @return {Promise<void>}
 */
const clearUpdatesRange = async (db, tableName, docName, from, to) => (
  clearRange(
    db,
    tableName,
    docName,
    createDocumentUpdateKey(docName, from),
    createDocumentUpdateKey(docName, to)
  )
)

/**
 * @param {any} db
 * @param {string} tableName
 * @param {string} docName
 * @param {Uint8Array} sv state vector
 * @param {number} clock current clock of the document so we can determine when this statevector was created
 */
const writeStateVector = async (db, tableName, docName, sv, clock) => {
  const encoder = encoding.createEncoder()
  encoding.writeVarUint(encoder, clock)
  encoding.writeVarUint8Array(encoder, sv)
  await dynamoDbPut(db, tableName, docName, createDocumentStateVectorKey(docName), encoding.toUint8Array(encoder))
}

/**
 * @param {Uint8Array} buf
 * @return {{ sv: Uint8Array, clock: number }}
 */
const decodeDynamoDbStateVector = (buf) => {
  const decoder = decoding.createDecoder(buf)
  const clock = decoding.readVarUint(decoder)
  const sv = decoding.readVarUint8Array(decoder)
  return { sv, clock }
}

/**
 * @param {any} db
 * @param {string} tableName
 * @param {string} docName
 */
const readStateVector = async (db, tableName, docName) => {
  const buf = await dynamoDbGet(db, tableName, docName, createDocumentStateVectorKey(docName))
  /* istanbul ignore if */
  if (buf === null) {
    // no state vector created yet or no document exists
    return { sv: null, clock: -1 }
  }
  return decodeDynamoDbStateVector(buf)
}

/**
 * @param {any} db
 * @param {string} tableName
 * @param {string} docName
 * @param {Uint8Array} stateAsUpdate
 * @param {Uint8Array} stateVector
 * @return {Promise<number>} returns the clock of the flushed doc
 */
const flushDocument = async (db, tableName, docName, stateAsUpdate, stateVector) => {
  const clock = await storeUpdate(db, tableName, docName, stateAsUpdate)
  await writeStateVector(db, tableName, docName, stateVector, clock)
  await clearUpdatesRange(db, tableName, docName, 0, clock)
  return clock
}

/**
 * @param {any} db
 * @param {string} tableName
 * @param {string} docName
 * @param {Uint8Array} update
 * @return {Promise<number>} Returns the clock of the stored update
 */
const storeUpdate = async (db, tableName, docName, update) => {
  const clock = await getCurrentUpdateClock(db, tableName, docName)
  if (clock === -1) {
    // make sure that a state vector is aways written, so we can search for available documents
    const ydoc = new Y.Doc()
    Y.applyUpdate(ydoc, update)
    const sv = Y.encodeStateVector(ydoc)
    await writeStateVector(db, tableName, docName, sv, 0)
  }
  await dynamoDbPut(db, tableName, docName, createDocumentUpdateKey(docName, clock + 1), update)
  return clock + 1
}

const DynamoDbPersistence = (config) => {
  const db = new DynamoDBClient(config.aws)
  let currentTransaction = Promise.resolve()

  // Execute an transaction on a database. This will ensure that other processes are currently not writing.
  const transact = (f) => {
    currentTransaction = currentTransaction.then(async () => {
      let res = null
      /* istanbul ignore next */
      try { res = await f(db, config) } catch (err) {
        console.warn('Error during y-dynamodb transaction', err)
      }
      return res
    })
    return currentTransaction
  }

  if (!config.skipCreateTable) {
    transact((db, config) => createTable(db, config))
  }

  const getYDoc = (docName, outsideTransactionQueue) => {
    const callback = async (db, config) => {
      const updates = await getDynamoDbUpdates(db, config.tableName, docName)
      const ydoc = new Y.Doc()
      ydoc.transact(() => {
        for (let i = 0; i < updates.length; i++) {
          Y.applyUpdate(ydoc, updates[i].value)
        }
      })
      if (updates.length > PREFERRED_TRIM_SIZE) {
        await flushDocument(db, config.tableName, docName, Y.encodeStateAsUpdate(ydoc), Y.encodeStateVector(ydoc))
      }
      return ydoc
    }

    return outsideTransactionQueue ? callback(db, config) : transact(callback)
  }

  return {
    transact,
    getYDoc,
    storeUpdate: (docName, update, outsideTransactionQueue) => {
      const callback = (db, config) => storeUpdate(db, config.tableName, docName, update)

      return outsideTransactionQueue ? callback(db, config) : transact(callback)
    },
    flushDocument: (docName, outsideTransactionQueue) => {
      const callback = async (db, config) => {
        const updates = await getDynamoDbUpdates(db, config.tableName, docName)
        const { update, sv } = mergeUpdates(updates)
        return flushDocument(db, config.tableName, docName, update, sv)
      }

      return outsideTransactionQueue ? callback(db, config) : transact(callback)
    },
    getStateVector: (docName, outsideTransactionQueue) => {
      const callback = async (db, config) => {
        const { clock, sv } = await readStateVector(db, config.tableName, docName)
        let curClock = -1
        /* istanbul ignore next */
        if (sv !== null) {
          curClock = await getCurrentUpdateClock(db, config.tableName, docName)
        }
        if (sv !== null && clock === curClock) {
          return sv
        } else {
          // current state vector is outdated
          const updates = await getDynamoDbUpdates(db, config.tableName, docName)
          const { update, sv } = mergeUpdates(updates)
          await flushDocument(db, config.tableName, docName, update, sv)
          return sv
        }
      }

      return outsideTransactionQueue ? callback(db, config) : transact(callback)
    },
    getDiff: async (docName, stateVector) => {
      const ydoc = await getYDoc(docName)
      return Y.encodeStateAsUpdate(ydoc, stateVector)
    },
    clearDocument: (docName, outsideTransactionQueue) => {
      const callback = async (db, config) => {
        await dynamoDbDelete(db, config.tableName, docName, createDocumentStateVectorKey(docName))
        await clearRange(db, config.tableName, docName, createDocumentFirstKey(docName), createDocumentLastKey(docName))
      }

      return outsideTransactionQueue ? callback(db, config) : transact(callback)
    },
    setMeta: (docName, metaKey, value, outsideTransactionQueue) => {
      const callback = (db, config) => dynamoDbPut(db, config.tableName, docName, createDocumentMetaKey(docName, metaKey), buffer.encodeAny(value))

      return outsideTransactionQueue ? callback(db, config) : transact(callback)
    },
    getMeta: (docName, metaKey, outsideTransactionQueue) => {
      const callback = async (db, config) => {
        const res = await dynamoDbGet(db, config.tableName, docName, createDocumentMetaKey(docName, metaKey))
        if (res === null) {
          return// return void
        }
        return buffer.decodeAny(res)
      }

      return outsideTransactionQueue ? callback(db, config) : transact(callback)
    },
    delMeta: (docName, metaKey, outsideTransactionQueue) => {
      const callback = (db, config) => dynamoDbDelete(db, config.tableName, docName, createDocumentMetaKey(docName, metaKey))

      return outsideTransactionQueue ? callback(db, config) : transact(callback)
    },
    getMetas: (docName, outsideTransactionQueue) => {
      const callback = async (db, config) => {
        const data = await getDynamoDbMetaData(db, config.tableName, docName)
        const metas = new Map()
        data.forEach(v => { metas.set(v.ykeysort[3], buffer.decodeAny(v.value)) })
        return metas
      }

      return outsideTransactionQueue ? callback(db, config) : transact(callback)
    }
  }
}

module.exports = DynamoDbPersistence
module.exports.PREFERRED_TRIM_SIZE = PREFERRED_TRIM_SIZE
module.exports.getDynamoDbUpdates = getDynamoDbUpdates
