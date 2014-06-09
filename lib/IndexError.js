
var util = require('util');

function IndexError(message,req,res) {
  Error.call(this);
  this.message = message;
  this.req = req;
  this.res = res;
}

util.inherits(IndexError, Error);

module.exports = IndexError;