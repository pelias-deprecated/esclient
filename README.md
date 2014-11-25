
## Installation

```bash
$ npm install pelias-esclient
```

[![NPM](https://nodei.co/npm/pelias-esclient.png?downloads=true&stars=true)](https://nodei.co/npm/pelias-esclient)

Note: you will need `node` and `npm` installed first.

The easiest way to install `node.js` is with [nave.sh](https://github.com/isaacs/nave) by executing `[sudo] ./nave.sh usemain stable`

### Configuration

see: https://github.com/pelias/config

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

## Contributing

Please fork and pull request against upstream master on a feature branch.

Pretty please; provide unit tests and script fixtures in the `test` directory.

### Running Unit Tests

```bash
$ npm test
```

### Continuous Integration

Travis tests every release against node version `0.10`

[![Build Status](https://travis-ci.org/pelias/esclient.png?branch=master)](https://travis-ci.org/pelias/esclient)