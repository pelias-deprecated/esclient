
var consoleLogInterval;
var operationsPerSecondInterval;
var totalStreams = 0;
var lastStats = {};

module.exports = function( statsStream, statsObj ){

  if( 'object' !== statsObj ){
    statsObj = {};
  }

  var streamid = ++totalStreams;
  statsObj.start = new Date();

  var oldOps = 0;
  operationsPerSecondInterval = setInterval( function(){
    var newOps = statsObj.indexed || 0;
    if( newOps ) statsObj.end = new Date();
    statsObj.iops = newOps - oldOps;
    oldOps = newOps;
  }, 1000 );

  if( !consoleLogInterval ){
    consoleLogInterval = setInterval( function(){
      statsObj.retry_rate = statsObj.retries ? ( statsObj.retries / statsObj.indexed ).toFixed(2) + '%' : 0;
      console.log( 'stats', JSON.stringify( statsObj, null, 2 ) );
    }, 500 );
  }

  statsStream.on( 'stats', function( stats ){

    lastStats[ streamid ] = stats;
    var newStats = {};

    for( var streamid in lastStats ){
      var stats = lastStats[ streamid ];
      for( var attr in stats ){
        if( 'number' == typeof newStats[attr] && 'number' == typeof stats[attr] ){
          newStats[attr] = newStats[attr] + stats[attr];
        } else {
          newStats[attr] = stats[attr];
        }
      }
    }
    for( var attr in statsObj ){
      delete statsObj[ attr ];
    }
    for( var attr in newStats ){
      statsObj[ attr ] = newStats[ attr ];
    }
  });

}