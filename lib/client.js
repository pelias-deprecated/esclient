
var elasticsearch = require('elasticsearch'),
    defaults = require('../config/defaults'),
    BufferedBulkStream = require('./BufferedBulkStream')
    livestats = require('./livestats');

module.exports = function(){

  var copyDefaults = {};
  for( var attr in defaults ){
    copyDefaults[ attr ] = defaults[ attr ];
  }

  // Create new esclient with default settings
  var client = new elasticsearch.Client( copyDefaults );

  // Provide a simple utility to convert client errors to standard nodejs interface (err,res)
  // @deprecated?
  client.errorHandler = function(cb) {
    return function(err, data, errcode) {
      if(err && err.message) {
        return cb(err.message, data);
      }
      return cb(null, data);
    }
  }

  // Create a streaming interface to the bulk API with buffering enabled
  client.stream = new BufferedBulkStream( client );
  client.livestats = livestats.bind( null, client.stream );

  return client;

}