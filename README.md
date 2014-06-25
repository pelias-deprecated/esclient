
### pelias-esclient

A nodejs streaming client for elasticsearch.

### Usage

This library provides an `elasticsearch client` which is configured for bulk imports.

The API is exactly the same as `https://github.com/elasticsearch/elasticsearch-js` with the addition of a buffered streaming import named `client.stream`.

```javascript
var esclient = require('pelias-esclient')();

var command = {
  _index: 'pelias', _type: 'myindex', _id: 'myrecordid',
  data: {
    my: 'properties'
  }
}

esclient.stream.write( command );
```

```javascript
var esclient = require('pelias-esclient')();

some_other_stream.pipe( esclient.stream );
```

The library will buffer the incoming stream of commands (as `objects` or `JSON` encoded strings) and buffers them (in batches of 1000 by default). It will then flush the records to elasticsearch using the bulk API.