
var elasticsearch = require('elasticsearch'),
    settings = require('pelias-config').generate(),
    BufferedBulkStream = require('./BufferedBulkStream'),
    livestats = require('./livestats');

module.exports = function( config ){

  // Create new esclient with settings
  var client = new elasticsearch.Client( settings.export().esclient || {} );

  // Provide a simple utility to convert client errors to standard nodejs interface (err,res)
  // @deprecated?
  client.errorHandler = function(cb) {
    throw new Error( 'esclient.errorHandler: this method has been deprecated' );
    // return function(err, data, errcode) {
    //   if(err && err.message) {
    //     return cb(err.message, data);
    //   }
    //   return cb(null, data);
    // };
  };

  // Create a streaming interface to the bulk API with buffering enabled
  client.stream = new BufferedBulkStream( client, config );
  client.livestats = livestats.bind( null, client.stream );

  return client;

};