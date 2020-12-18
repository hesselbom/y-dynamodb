# DynamoDB database adapter for [Yjs](https://github.com/yjs/yjs)

Rewritten from [y-leveldb](https://github.com/yjs/y-leveldb/) to use AWS DynamoDB.

WIP.

```sh
docker-compose -f docker-compose-dynamodb-local.yml up

node ./testrun

aws dynamodb list-tables --endpoint-url http://localhost:8000 --region us-west-2
aws dynamodb scan --table-name test-y-dynamodb --endpoint-url http://localhost:8000 --region us-west-2
```

## Use it

```sh
npm install y-dynamodb --save
```

```js
const DynamoDbPersistence = require('./src/y-dynamodb')
const Y = require('yjs')

const config = {
  aws: {
    region: 'us-west-2',
    accessKeyId: 'accessKeyId',
    secretAccessKey: 'secretAccessKey',
    endpoint: 'http://localhost:8000'
  },
  skipCreateTable: true, // skips creating table, assumes it already exists
  tableName: 'test-y-dynamodb'
}
const persistence = DynamoDbPersistence(config)

const ydoc = new Y.Doc()
ydoc.getArray('arr').insert(0, [1, 2, 3])
console.log(ydoc.getArray('arr').toArray()) // => [1, 2, 3]

// store document updates retrieved from other clients
persistence.storeUpdate('my-doc', Y.encodeStateAsUpdate(ydoc))

// when you want to sync, or store data to a database,
// retrieve the temporary Y.Doc to consume data
persistence.getYDoc('my-doc')
  .then(doc => console.log('Got my-doc', doc.getArray('arr').toArray())) // [1, 2, 3]
```

## API

### `persistence = DynamoDbPersistence(config)`

Create a y-dynamodb persistence instance.

#### `persistence.getYDoc(docName: string): Promise<Y.Doc>`

Create a Y.Doc instance with the data persisted in DynamoDB. Use this to
temporarily create a Yjs document to sync changes or extract data.

#### `persistence.storeUpdate(docName: string, update: Uint8Array): Promise`

Store a single document update to the database.

#### `persistence.getStateVector(docName: string): Promise<Uint8Array>`

The state vector (describing the state of the persisted document - see
[Yjs docs](https://github.com/yjs/yjs#Document-Updates)) is maintained in a separate
field and constantly updated.

This allows you to sync changes without actually creating a Yjs document.

#### `persistence.getDiff(docName: string, stateVector: Uint8Array): Promise<Uint8Array>`

Get the differences directly from the database. The same as
`Y.encodeStateAsUpdate(ydoc, stateVector)`.

#### `persistence.clearDocument(docName: string): Promise`

Delete a document, and all associated data from the database.

#### `persistence.setMeta(docName: string, metaKey: string, value: any): Promise`

Persist some meta information in the database and associate it with a document.
It is up to you what you store here. You could, for example, store credentials
here.

#### `persistence.getMeta(docName: string, metaKey: string): Promise<any|undefined>`

Retrieve a store meta value from the database. Returns undefined if the
`metaKey` doesn't exist.

#### `persistence.getMetas(docName: string): Promise<Map<string, any>>`

Retries all meta keys and values from the database for a document.

#### `persistence.delMeta(docName: string, metaKey: string): Promise`

Delete a store meta value.

#### `persistence.flushDocument(docName: string): Promise` (dev only)

Internally y-dynamodb stores incremental updates. You can merge all document
updates to a single entry. You probably never have to use this.

## License

y-dynamodb is licensed under the [MIT License](./LICENSE).

<viktor@hesselbom.net>
