
var esclient = require('../');

module.exports.interface = {};

module.exports.interface.client = function(test, common) {
  test('client', function(t) {
    t.equal(typeof esclient, 'function', 'valid function');
    t.end();
  });
}

module.exports.all = function (tape, common) {

  function test(name, testFunction) {
    return tape('external interface: ' + name, testFunction)
  }

  for( var testCase in module.exports.interface ){
    module.exports.interface[testCase](test, common);
  }
}