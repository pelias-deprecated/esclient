
var operationsPerSecondInterval;
var totalStreams = 0;
var lastStats = {};

module.exports = function( statsStream, statsObj ){

  if( 'object' !== statsObj ){
    statsObj = {};
  }

  var streamid = ++totalStreams;

  var oldOps = 0;
  operationsPerSecondInterval = setInterval( function(){
    var newOps = statsObj.indexed || 0;
    statsObj.iops = newOps - oldOps;
    statsObj.retry_rate = statsObj.retries ? ( 100 / (
      ( statsObj.written + statsObj.retries ) / statsObj.retries
    ) ).toFixed(2) + '%' : 0;
    oldOps = newOps;
    console.log( 'stats', JSON.stringify( statsObj, null, 2 ) );
  }, 1000 );

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

  statsStream.on( 'release', function(){
    clearInterval( operationsPerSecondInterval );
  });

}