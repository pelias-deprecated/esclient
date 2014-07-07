
### pelias-esclient

A nodejs streaming client for elasticsearch.

### Configuration

The client comes with some sane [defaults](https://github.com/mapzen/pelias-esclient/blob/master/config/defaults.json) for testing on your local machine. If you wish to override any of the default settings you can create a `json` file which contains the properties you wish to override the default config.

You must tell `esclient` where your configuartion is stored using an environment variable:
```bash
PELIAS_CONFIG=/tmp/pelias.json node script.js
```

Full configuration reference: http://www.elasticsearch.org/guide/en/elasticsearch/client/javascript-api/current/configuration.html

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

The library will buffer the incoming stream of commands (as `objects` or `JSON` encoded strings) and buffers them (in batches of 500 by default). It will then flush the records to elasticsearch using the bulk API.