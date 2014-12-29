
var elasticsearch = require('elasticsearch'),
    BufferedBulkStream = require('./BufferedBulkStream'),
    livestats = require('./livestats'),
    isObject = require('is-object');

/**
 * @param {object} config Properties to configure BufferedBulkStream
 * @param {object} settings Properties to configure elasticsearch.Client
 */

module.exports = function( config, settings ){

  // @note API change as of v0.0.26+
  if( !isObject( settings ) ){
    throw new Error( 'elasticsearch.Client: elasticsearch client settings mandatory as of v0.0.26+' );
  }

  // Create new esclient with settings
  var client = new elasticsearch.Client( settings );

  // Create a streaming interface to the bulk API with buffering enabled
  client.stream = new BufferedBulkStream( client, config );
  client.livestats = livestats.bind( null, client.stream );

  return client;

};