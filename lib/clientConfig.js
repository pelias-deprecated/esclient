
// elasticsearch.Client mutates it's config so we
// need to copy the defaults object
  
var Config = function(){
  this.mergeConfig( '../config/defaults' );
  this.mergeConfig( process.env.PELIAS_CONFIG, 'esclient' );
}

Config.prototype.mergeConfig = function( path, prop ){
  var merge = function( config ){
    for( var attr in config ){
      this[ attr ] = config[ attr ];
    }
  }.bind(this);
  try {
    var config = require( path );
    if( 'object' == typeof config ){
      if( 'string' == typeof prop ){
        if( 'object' == typeof config[prop] ){
          return merge( config[prop] );
        }
      }
      return merge( config );
    }
  } catch( e ){} // silent fail
}

module.exports = Config;