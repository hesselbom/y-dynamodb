const DynamoDbPersistence = require('./src/y-dynamodb')
const Y = require('yjs')

const config = {
  aws: {
    region: 'us-west-2',
    endpoint: 'http://localhost:8000'
  },
  tableName: 'test-y-dynamodb'
}
const persistence = DynamoDbPersistence(config)

// Clear before creation
// persistence.clearDocument('my-doc').then(() => console.log('Cleared before creation'))

const ydoc = new Y.Doc()
ydoc.getArray('arr').insert(0, [1, 2, 3])
console.log(ydoc.getArray('arr').toArray()) // => [1, 2, 3]

const ydoc2 = new Y.Doc()
ydoc2.getArray('arr').insert(0, [4, 5, 6])
console.log(ydoc2.getArray('arr').toArray()) // => [4, 5, 6]

// store document updates retrieved from other clients
persistence.storeUpdate('my-doc', Y.encodeStateAsUpdate(ydoc))
persistence.storeUpdate('my-doc2', Y.encodeStateAsUpdate(ydoc2))

// when you want to sync, or store data to a database,
// retrieve the temporary Y.Doc to consume data
persistence.getYDoc('my-doc')
  .then(doc => console.log('Got my-doc', doc.getArray('arr').toArray())) // [1, 2, 3]
persistence.getYDoc('my-doc2')
  .then(doc => console.log('Got my-doc2', doc.getArray('arr').toArray())) // [4, 5, 6]

persistence.clearDocument('my-doc').then(() => console.log('Cleared'))
persistence.getYDoc('my-doc').then(doc => console.log('Got my-doc', doc.getArray('arr').toArray()))
persistence.getYDoc('my-doc2').then(doc => console.log('Got my-doc2', doc.getArray('arr').toArray()))

// persistence.getStateVector('my-doc').then(vector => {
//   console.log('got vector', vector)
//
//   persistence.getDiff('my-doc', vector).then(diff => {
//     console.log('diff', diff)
//   })
// })

// persistence.flushDocument('my-doc').then(clock => {
//   console.log('flushed document', clock)
// })
//
// persistence.getYDoc('my-doc')
//   .then(doc => console.log('Got my-doc again', doc.getArray('arr').toArray())) // [1, 2, 3]
//
// persistence.getMetas('my-doc').then(metas => console.log('Got metas', metas))
//
// persistence.setMeta('my-doc', 'my-key', 'my-value').then(() => console.log('Wrote meta'))
// persistence.getMeta('my-doc', 'my-key').then(meta => console.log('Got meta:', meta))
// persistence.getMetas('my-doc').then(metas => console.log('Got metas:', metas))
//
// persistence.delMeta('my-doc', 'my-key').then(() => console.log('Removed meta'))
// persistence.getMeta('my-doc', 'my-key').then(meta => console.log('Got meta:', meta))
// persistence.getMetas('my-doc').then(metas => console.log('Got metas:', metas))
