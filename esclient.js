
// @todo: this file requires a clean-up
// @todo: refactor write() calls to accept all batch actions instead of just 'index'

var elasticsearch = require('elasticsearch'),
    Writable = require('stream').Writable;

var client = new elasticsearch.Client({
  apiVersion: '1.1',
  keepAlive: true,
  hosts: [{
    env: 'development',
    protocol: 'http',
    host: 'localhost',
    port: 9200
  }],
  log: [{
    type: 'stdio',
    level: [ 'error', 'warning' ]
  },{
    type: 'file',
    level: [ 'trace' ],
    path: 'esclient.log'
  }]
});

client.errorHandler = function(cb) {
  return function(err, data, errcode) {
    if(err && err.message) {
      return cb(err.message, data);
    }
    return cb(null, data);
  }
}

// streaming interface
client.stream = new Writable({ objectMode: true });

var buff = [];
var bufferMaxSize = 500;
var writtenCount = 0;
var insertedCount = 0;
var lastFlush = new Date().getTime();

// Flush buffer to ES
var flush = function(cb)
{
  if( !buff.length ) return;
  
  writtenCount += ( buff.length / 2 );

  client.bulk({ body: buff }, function( err, resp ){
    // console.log( err, resp );
    insertedCount += ( buff.length / 2 );
    buff = []; // Reset buffer
    if( cb ) cb();
  });

  console.log( 'writing %s records to ES', ( buff.length / 2 ), {
    sent: writtenCount,
    saved: insertedCount,
    queuedInElasticSearch: writtenCount - insertedCount
  });

  lastFlush = new Date().getTime()
}

client.stream._write = function (chunk, enc, next)
{
  // stream accepts objects or json encoded strings
  var record = ( 'object' === typeof chunk ) ? chunk : JSON.parse( chunk.toString() );
  
  buff.push({ index: {
    _index: record._index,
    _type: record._type,
    _id: record._id
  }}, record.data );
  
  // Buffer not full yet
  if( ( buff.length / 2 ) < bufferMaxSize ){
    return next();
  }

  flush(next);
};

client.stream.on( 'error', console.log.bind(console) );

// @todo: flush a half full buffer instead of waiting for more data to come in.
// This is currently a massive hack to make sure the buffer is flushed in 
// a timely manner. It needs to go away with the next refactor.
setInterval( function(){
  if(( new Date().getTime()-1000 ) > lastFlush ){
    flush();
  }
}, 1000 );

// try to flush when the stream ends
client.stream.on( 'end', function(){
  flush( function(){
    console.log( 'finished writing data to ES' );
  });
});

module.exports = client;