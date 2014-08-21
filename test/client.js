
var esclient = require('../'),
    elasticsearch = require('elasticsearch');

module.exports.client = {};

module.exports.client.generated = function(test, common) {
  test('generated', function(t) {
    var client = esclient();
    t.equal(typeof client, 'object', 'valid object');
    t.end();
  });
}

module.exports.client.augments = function(test, common) {
  test('augments elasticsearch client', function(t) {
    var es = new elasticsearch.Client();
    var client = esclient();
    var added = [ 'errorHandler', 'stream', 'livestats' ];
    t.deepEqual(Object.keys(es).concat(added), Object.keys(client), 'valid method signature');
    t.end();
  });
}

module.exports.client.stream = function(test, common) {
  test('stream', function(t) {
    var client = esclient();
    t.equal(typeof client.stream, 'object', 'valid function');
    t.equal(typeof client.stream._write, 'function', 'writable stream');
    t.end();
  });
}

module.exports.all = function (tape, common) {

  function test(name, testFunction) {
    return tape('client: ' + name, testFunction)
  }

  for( var testCase in module.exports.client ){
    module.exports.client[testCase](test, common);
  }
}