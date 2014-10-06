
var util = require('util'),
    stream = require('stream'),
    IndexError = require('./IndexError');

var BufferedBulkStream = function( client, options )
{
  this.client = client;
  this.buffer = [];
  this.flooding = false;
  this.options = { buffer: 500, timeout: 1000, throttle: 5, maxThrottle: 100 };
  this.stats = { written: 0, inserted: 0, ok: 0, error: 0, active_requests: 0, retries: 0 };
  this.timeout = null;

  // merge injected options
  if( 'object' == typeof options ){
    for( var prop in options ) {
      this.options[ prop ] = options[ prop ];
    }
  }

  // Set current throttle
  this.currentThrottle = this.options.throttle;

  // Call constructor from stream.Writable
  stream.Writable.call( this, { objectMode: true } );

  // Log errors to stderr
  this.on( 'failure', console.error.bind( console ) );

  // Try to flush when the stream ends
  this.on( 'end', this.flush.bind( this, true ) );
  this.on( 'close', this.flush.bind( this, true ) );

  // resume function (for flood control)
  this.resumeFunction = function(){};

  // allow the client to exit cleanly when complete
  this.on( 'finish', function(){
    var interval;
    this.resumeFunction = function(){}; // disable calls to next();

    var release = function(){
      clearInterval( interval );
      if( !this.buffer.length && !this.stats.active_requests ){
        this.client.close(); // close the underlying client
        this.emit( 'release' );
      } else {
        interval = setInterval( release, 1000 );
        this.flush( true );
      }
    }.bind( this );

    release();
  });
};

// Inherit prototype from stream.Writable
util.inherits( BufferedBulkStream, stream.Writable );

// Backoff on the 'throttle' with failures, scale up with successes.
BufferedBulkStream.prototype.batch = function( hasFailures ){
  if( hasFailures ){
    this.currentThrottle -= 2;
    if( this.currentThrottle < this.options.throttle ){
      this.currentThrottle = this.options.throttle;
    }
  }
  else if( Math.random() > 0.9 ){
    if( this.currentThrottle < this.options.maxThrottle ){
      this.currentThrottle++;
    }
  }
};

// Handle new messages on the stream
BufferedBulkStream.prototype._write = function( chunk, enc, next )
{
  // BufferedBulkStream accepts Objects or JSON encoded strings
  var record = this.parseChunk( chunk );

  // Record message error stats
  this.stats[ record? 'ok':'error' ]++;

  if( record )
  {
    // Push command to buffer
    this.buffer.push({ index: {
      _index: record._index,
      _type: record._type,
      _id: record._id
    }}, record.data );
    
    this.flush();
  }

  // Handle partial flushes due to inactivity
  clearTimeout( this.timeout );
  this.timeout = setTimeout( function(){
    this.flush( true );
  }.bind(this), this.options.timeout );

  this.resumeFunction = next;
  if( !this.flooding ) next();
};

// Flush buffer to client
BufferedBulkStream.prototype.flush = function( force )
{
  // Buffer not full
  if( this.buffer.length < 2 ){ return; } // Prevent 'Failed to derive xcontent from org.elasticsearch.common.bytes.BytesArray@0'
  if( !force && ( this.buffer.length / 2 ) < this.options.buffer ){ return; }

  // Move commands out of main buffer
  var writeBuffer = this.buffer.splice( 0, this.options.buffer * 2 );
  writeBufferTotal = ( writeBuffer.length / 2 );
  
  // Write buffer to client
  this.stats.written += writeBufferTotal;
  this.stats.active_requests++;

  // flood control backoff
  if( this.stats.active_requests >= this.currentThrottle ){
    this.flooding = true;
  }

  this.client.bulk( { body: writeBuffer }, function( err, resp ){

    this.stats.active_requests--;

    // major error
    // @todo: retry whole batch?
    if( err ){
      this.stats.error += writeBufferTotal;
       return this.emit( 'failure', new IndexError( err || 'bulk index error', null, resp ));
    }

    // response does not contain items
    // @todo: retry whole batch?
    if( !resp || !resp.items ){
      this.stats.error += writeBufferTotal; // consider them as failed
      return this.emit( 'failure', new IndexError( 'invalid resp from es bulk index operation', null, resp ));
    }

    // process response
    this.validateBulkResponse( writeBuffer, resp );

    // flood control resume
    if( this.stats.active_requests < this.currentThrottle && this.flooding ){
      this.flooding = false;
      if( 'function' == typeof this.resumeFunction ) this.resumeFunction();
    }

    // Stats
    this.emitStats();

  }.bind(this));

  // Stats
  this.emitStats();
};

BufferedBulkStream.prototype.validateBulkResponse = function( writeBuffer, resp ){

  // create a map of response codes -> query positions
  var responseCodes = resp.items.reduce( function( codes, item, i ){
    if( !codes[ item.index.status ] ){ codes[ item.index.status ] = []; }
    codes[ item.index.status ].push( i );
    return codes;
  }, {});

  this.batch( responseCodes.hasOwnProperty('503') );

  // iterate over ES responses for each item in bulk request
  for( var code in responseCodes ){

    // Successfully updated index
    if( code == '200' ){
      this.stats.inserted += responseCodes['200'].length;
    }

    // Successfully created index
    else if( code == '201' ){
      this.stats.inserted += responseCodes['201'].length;
    }

    // Retry-able failure
    else if( code == '503' ){
      responseCodes['503'].forEach( function( i ){
        var startIndex = i * 2;
        // Push back in to buffer to try again
        this.buffer.push(
          writeBuffer.slice( startIndex +0, startIndex +1 )[0],
          writeBuffer.slice( startIndex +1, startIndex +2 )[0]
        );
        this.stats.retries++;
      }, this);
    }

    // Elasticsearch returned an error
    else if( code == '400' ){
      this.stats.error += responseCodes['400'].length;

      // Format error info and emit
      responseCodes['400'].forEach( function( i ){
        this.emit( 'failure', new IndexError(
          resp.items[ i ].index.error,
          writeBuffer.slice( (i*2)+1, (i*2)+2 )[0],
          resp.items[ i ]
        ));
      }, this);
    }

    // Unknown response code
    else {
      this.stats.error += responseCodes[code].length;

      // Format error info and emit
      responseCodes[code].forEach( function( i ){
        this.emit( 'failure', new IndexError(
          'unknown response code',
          writeBuffer.slice( (i*2)+1, (i*2)+2 )[0],
          resp.items[ i ]
        ));
      }, this);
    }

  }
};

// Stats
BufferedBulkStream.prototype.emitStats = function()
{
  this.emit( 'stats', {
    written: this.stats.written - this.stats.retries,
    indexed: this.stats.inserted,
    errored: this.stats.error,
    retries: this.stats.retries,
    active_requests: this.stats.active_requests,
    queued: this.stats.written - this.stats.inserted - this.stats.error - this.stats.retries
  });
};

// BufferedBulkStream accepts Objects or JSON encoded strings
BufferedBulkStream.prototype.parseChunk = function( chunk )
{
  if( 'string' === typeof chunk ){
    try {
      chunk = JSON.parse( chunk.toString() );
    } catch( e ){
      this.emit( 'failure', 'failed to parse JSON chunk' );
      return;
    }
  }
  
  if( 'object' === typeof chunk ){
    if( !chunk._index ){
      this.emit( 'failure', 'invalid index specified' );
      return;
    } else if( !chunk._type ){
      this.emit( 'failure', 'invalid type specified' );
      return;
    } else if( !chunk._id ){
      this.emit( 'failure', 'invalid id specified' );
      return;
    }
    
    // Chunk is valid
    return chunk;
  }

  this.emit( 'failure', 'invalid bulk API message' );
  return;
};

module.exports = BufferedBulkStream;